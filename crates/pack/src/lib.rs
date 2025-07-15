/*
 * Copyright (C) 2009 Jelmer Vernooij <jelmer@jelmer.uk>
 *
 * Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
 * General Public License as public by the Free Software Foundation; version 2.0
 * or (at your option) any later version. You can redistribute it and/or
 * modify it under the terms of either of these two licenses.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You should have received a copy of the licenses; if not, see
 * <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
 * and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
 * License, Version 2.0.
 */

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use std::cmp::min;
use std::collections::HashMap;

pyo3::import_exception!(dulwich.errors, ApplyDeltaError);

fn py_is_sha(sha: &PyObject, py: Python) -> PyResult<bool> {
    // Check if the object is a bytes object
    if sha.bind(py).is_instance_of::<PyBytes>() {
        // Check if the bytes object has a size of 20
        if sha.extract::<&[u8]>(py)?.len() == 20 {
            Ok(true)
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

#[pyfunction]
fn bisect_find_sha(
    py: Python,
    start: i32,
    end: i32,
    sha: Py<PyBytes>,
    unpack_name: PyObject,
) -> PyResult<Option<i32>> {
    // Convert sha_obj to a byte slice
    let sha = sha.as_bytes(py);
    let sha_len = sha.len();

    // Check if sha is 20 bytes long
    if sha_len != 20 {
        return Err(PyValueError::new_err("Sha is not 20 bytes long"));
    }

    // Check if start > end
    if start > end {
        return Err(PyValueError::new_err("start > end"));
    }

    // Binary search loop
    let mut start = start;
    let mut end = end;
    loop {
        if start > end {
            break;
        }
        let i = (start + end) / 2;

        let file_sha = unpack_name.call1(py, (i,))?;
        if !py_is_sha(&file_sha, py)? {
            return Err(PyTypeError::new_err("unpack_name returned non-sha object"));
        }

        match file_sha.extract::<&[u8]>(py).unwrap().cmp(sha) {
            std::cmp::Ordering::Less => {
                start = i + 1;
            }
            std::cmp::Ordering::Greater => {
                end = i - 1;
            }
            std::cmp::Ordering::Equal => {
                return Ok(Some(i));
            }
        }
    }

    Ok(None)
}

fn get_delta_header_size(delta: &[u8], index: &mut usize, length: usize) -> usize {
    let mut size: usize = 0;
    let mut i: usize = 0;
    while *index < length {
        let cmd = delta[*index];
        *index += 1;
        size |= ((cmd & !0x80) as usize) << i;
        i += 7;
        if cmd & 0x80 == 0 {
            break;
        }
    }
    size
}

fn py_chunked_as_string<'a>(
    py: Python<'a>,
    py_buf: &'a PyObject,
) -> PyResult<std::borrow::Cow<'a, [u8]>> {
    if let Ok(py_list) = py_buf.extract::<Bound<PyList>>(py) {
        let mut buf = Vec::new();
        for chunk in py_list.iter() {
            if let Ok(chunk) = chunk.extract::<&[u8]>() {
                buf.extend_from_slice(chunk);
            } else if let Ok(chunk) = chunk.extract::<Vec<u8>>() {
                buf.extend(chunk);
            } else {
                return Err(PyTypeError::new_err(format!(
                    "chunk is not a byte string, but a {:?}",
                    chunk.get_type().name()
                )));
            }
        }
        Ok(buf.into())
    } else if py_buf.extract::<Bound<PyBytes>>(py).is_ok() {
        Ok(std::borrow::Cow::Borrowed(py_buf.extract::<&[u8]>(py)?))
    } else {
        Err(PyTypeError::new_err(
            "buf is not a string or a list of chunks",
        ))
    }
}

#[pyfunction]
fn apply_delta(py: Python, py_src_buf: PyObject, py_delta: PyObject) -> PyResult<Vec<PyObject>> {
    let src_buf = py_chunked_as_string(py, &py_src_buf)?;
    let delta = py_chunked_as_string(py, &py_delta)?;

    let src_buf_len = src_buf.len();
    let delta_len = delta.len();
    let mut index = 0;

    let src_size = get_delta_header_size(delta.as_ref(), &mut index, delta_len);
    if src_size != src_buf_len {
        return Err(ApplyDeltaError::new_err(format!(
            "Unexpected source buffer size: {} vs {}",
            src_size, src_buf_len
        )));
    }

    let dest_size = get_delta_header_size(delta.as_ref(), &mut index, delta_len);
    let mut out = vec![0; dest_size];
    let mut outindex = 0;

    while index < delta_len {
        let cmd = delta[index];
        index += 1;

        if cmd & 0x80 != 0 {
            let mut cp_off = 0;
            let mut cp_size = 0;

            for i in 0..4 {
                if cmd & (1 << i) != 0 {
                    let x = delta[index] as usize;
                    index += 1;
                    cp_off |= x << (i * 8);
                }
            }

            for i in 0..3 {
                if cmd & (1 << (4 + i)) != 0 {
                    let x = delta[index] as usize;
                    index += 1;
                    cp_size |= x << (i * 8);
                }
            }

            if cp_size == 0 {
                cp_size = 0x10000;
            }

            if cp_off + cp_size < cp_size || cp_off + cp_size > src_size || cp_size > dest_size {
                break;
            }

            out[outindex..outindex + cp_size].copy_from_slice(&src_buf[cp_off..cp_off + cp_size]);
            outindex += cp_size;
        } else if cmd != 0 {
            if (cmd as usize) > dest_size {
                break;
            }

            // Raise ApplyDeltaError if there are more bytes to copy than space
            if outindex + cmd as usize > dest_size {
                return Err(ApplyDeltaError::new_err("Not enough space to copy"));
            }

            out[outindex..outindex + cmd as usize]
                .copy_from_slice(&delta[index..index + cmd as usize]);
            outindex += cmd as usize;
            index += cmd as usize;
        } else {
            return Err(ApplyDeltaError::new_err("Invalid opcode 0"));
        }
    }

    if index != delta_len {
        return Err(ApplyDeltaError::new_err("delta not empty"));
    }

    if outindex != dest_size {
        return Err(ApplyDeltaError::new_err("dest size incorrect"));
    }

    Ok(vec![PyBytes::new(py, &out).into()])
}

/// Maximum copy length for a single copy operation - 64KB
const MAX_COPY_LEN: usize = 0xFFFF;

/// Encodes a size using Git's variable-length encoding format
fn delta_encode_size(mut size: usize) -> Vec<u8> {
    let mut result = Vec::new();
    let mut c = (size & 0x7F) as u8;
    size >>= 7;

    while size > 0 {
        result.push(c | 0x80);
        c = (size & 0x7F) as u8;
        size >>= 7;
    }
    result.push(c);
    result
}

/// Encodes a copy operation as bytes
fn encode_copy_operation(start: usize, length: usize) -> Vec<u8> {
    let mut result = vec![0x80u8];

    // Encode start offset (up to 4 bytes)
    for i in 0..4 {
        let byte_val = (start >> (i * 8)) & 0xFF;
        if byte_val != 0 {
            result.push(byte_val as u8);
            result[0] |= 1 << i;
        }
    }

    // Encode length (up to 2 bytes)
    for i in 0..2 {
        let byte_val = (length >> (i * 8)) & 0xFF;
        if byte_val != 0 {
            result.push(byte_val as u8);
            result[0] |= 1 << (4 + i);
        }
    }

    result
}

/// Delta operation types
#[derive(Debug, Clone, Copy, PartialEq)]
enum DeltaOpcode {
    Equal,
    Insert,
}

/// Delta operation with positions
#[derive(Debug, Clone)]
struct DeltaOperation {
    opcode: DeltaOpcode,
    base_start: usize,
    base_end: usize,
    target_start: usize,
    target_end: usize,
}

/// Sophisticated diff algorithm using hash-based matching
fn find_delta_operations(base: &[u8], target: &[u8]) -> Vec<DeltaOperation> {
    let mut operations = Vec::new();

    // Build a hash map of base buffer for efficient searching
    let mut base_map: HashMap<&[u8], Vec<usize>> = HashMap::new();
    let min_match_len = 4; // Minimum match length to consider

    // Index base buffer with sliding windows
    for i in 0..=base.len().saturating_sub(min_match_len) {
        let end = std::cmp::min(i + 32, base.len()); // Limit window size
        let window = &base[i..end];
        base_map.entry(window).or_insert_with(Vec::new).push(i);
    }

    let mut target_pos = 0;

    while target_pos < target.len() {
        let mut best_match: Option<(usize, usize, usize)> = None;

        // Try different window sizes to find the best match
        for window_size in (min_match_len..=std::cmp::min(32, target.len() - target_pos)).rev() {
            if target_pos + window_size <= target.len() {
                let target_window = &target[target_pos..target_pos + window_size];

                if let Some(base_positions) = base_map.get(target_window) {
                    for &base_start in base_positions {
                        // Try to extend the match
                        let mut match_len = 0;
                        let max_extend =
                            std::cmp::min(base.len() - base_start, target.len() - target_pos);

                        while match_len < max_extend
                            && base[base_start + match_len] == target[target_pos + match_len]
                        {
                            match_len += 1;
                        }

                        if match_len >= min_match_len
                            && (best_match.is_none() || match_len > best_match.unwrap().2)
                        {
                            best_match = Some((base_start, target_pos, match_len));
                        }
                    }
                }

                if best_match.is_some() {
                    break; // Found a good match, don't try smaller windows
                }
            }
        }

        if let Some((base_start, target_start, match_len)) = best_match {
            // Add copy operation
            operations.push(DeltaOperation {
                opcode: DeltaOpcode::Equal,
                base_start,
                base_end: base_start + match_len,
                target_start,
                target_end: target_start + match_len,
            });
            target_pos += match_len;
        } else {
            // No match found, find end of literal run
            let literal_start = target_pos;
            target_pos += 1;

            // Continue until we find a potential match
            while target_pos < target.len() {
                let mut found_match = false;
                for window_size in
                    (min_match_len..=std::cmp::min(16, target.len() - target_pos)).rev()
                {
                    if target_pos + window_size <= target.len() {
                        let window = &target[target_pos..target_pos + window_size];
                        if base_map.contains_key(window) {
                            found_match = true;
                            break;
                        }
                    }
                }
                if found_match {
                    break;
                }
                target_pos += 1;
            }

            // Add insert operation for literal data
            operations.push(DeltaOperation {
                opcode: DeltaOpcode::Insert,
                base_start: 0,
                base_end: 0,
                target_start: literal_start,
                target_end: target_pos,
            });
        }
    }

    operations
}

/// Creates a Git-compatible delta between base and target buffers
fn create_delta_internal(base_buf: &[u8], target_buf: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    // Write delta header - encoded sizes of base and target
    result.extend(delta_encode_size(base_buf.len()));
    result.extend(delta_encode_size(target_buf.len()));

    // Generate diff operations
    let operations = find_delta_operations(base_buf, target_buf);

    for operation in operations {
        match operation.opcode {
            DeltaOpcode::Equal => {
                // Copy operation - reference data from base buffer
                let mut copy_start = operation.base_start;
                let mut copy_len = operation.base_end - operation.base_start;

                while copy_len > 0 {
                    let to_copy = min(copy_len, MAX_COPY_LEN);
                    result.extend(encode_copy_operation(copy_start, to_copy));
                    copy_start += to_copy;
                    copy_len -= to_copy;
                }
            }
            DeltaOpcode::Insert => {
                // Insert operation - include literal data from target
                let mut remaining = operation.target_end - operation.target_start;
                let mut offset = operation.target_start;

                while remaining > 127 {
                    result.push(127);
                    result.extend_from_slice(&target_buf[offset..offset + 127]);
                    remaining -= 127;
                    offset += 127;
                }

                if remaining > 0 {
                    result.push(remaining as u8);
                    result.extend_from_slice(&target_buf[offset..offset + remaining]);
                }
            }
        }
    }

    result
}

#[pyfunction]
fn create_delta(py: Python, py_base_buf: PyObject, py_target_buf: PyObject) -> PyResult<PyObject> {
    let base_buf = py_chunked_as_string(py, &py_base_buf)?;
    let target_buf = py_chunked_as_string(py, &py_target_buf)?;

    let delta = create_delta_internal(base_buf.as_ref(), target_buf.as_ref());

    // Return a list with the delta as a single chunk to maintain compatibility
    let py_list = PyList::empty(py);
    py_list.append(PyBytes::new(py, &delta))?;
    Ok(py_list.into())
}

#[pymodule]
fn _pack(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bisect_find_sha, m)?)?;
    m.add_function(wrap_pyfunction!(apply_delta, m)?)?;
    m.add_function(wrap_pyfunction!(create_delta, m)?)?;
    Ok(())
}
