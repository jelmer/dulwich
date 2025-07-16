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

            if cp_off.saturating_add(cp_size) < cp_size
                || cp_off + cp_size > src_size
                || cp_size > dest_size
            {
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

/// Rabin fingerprinting constants
const RABIN_WINDOW_SIZE: usize = 16;
const RABIN_SHIFT: u64 = 23;

/// Rabin fingerprint structure for efficient string matching
#[derive(Debug, Clone)]
struct RabinIndex {
    /// Maps fingerprint to list of positions in base buffer
    fingerprints: HashMap<u64, Vec<usize>>,
}

impl RabinIndex {
    fn new(base: &[u8]) -> Self {
        let mut fingerprints = HashMap::new();

        if base.len() < RABIN_WINDOW_SIZE {
            return RabinIndex { fingerprints };
        }

        // Calculate initial fingerprint
        let mut hash = 0u64;
        for item in base.iter().take(RABIN_WINDOW_SIZE) {
            hash = hash.wrapping_mul(RABIN_SHIFT).wrapping_add(*item as u64);
        }

        fingerprints.entry(hash).or_insert_with(Vec::new).push(0);

        // Rolling hash for remaining positions
        let shift_value = (1u64 << RABIN_SHIFT).wrapping_sub(1);
        for i in RABIN_WINDOW_SIZE..base.len() {
            // Remove leftmost character and add rightmost
            let old_char = base[i - RABIN_WINDOW_SIZE] as u64;
            let new_char = base[i] as u64;

            hash = hash
                .wrapping_sub(old_char.wrapping_mul(shift_value))
                .wrapping_mul(RABIN_SHIFT)
                .wrapping_add(new_char);

            let pos = i - RABIN_WINDOW_SIZE + 1;
            fingerprints.entry(hash).or_insert_with(Vec::new).push(pos);
        }

        RabinIndex { fingerprints }
    }

    fn find_matches(&self, target: &[u8], target_pos: usize) -> Vec<usize> {
        if target_pos + RABIN_WINDOW_SIZE > target.len() {
            return Vec::new();
        }

        // Calculate fingerprint for target window
        let mut hash = 0u64;
        for item in target.iter().skip(target_pos).take(RABIN_WINDOW_SIZE) {
            hash = hash.wrapping_mul(RABIN_SHIFT).wrapping_add(*item as u64);
        }

        self.fingerprints.get(&hash).cloned().unwrap_or_default()
    }
}

/// Advanced diff algorithm using Rabin fingerprinting
fn find_delta_operations(base: &[u8], target: &[u8]) -> Vec<DeltaOperation> {
    let mut operations = Vec::new();
    let min_match_len = 3;

    // Build Rabin fingerprint index for base buffer
    let rabin_index = RabinIndex::new(base);

    // Also build a simple hash map for short matches
    let mut short_map: HashMap<&[u8], Vec<usize>> = HashMap::new();
    for i in 0..=base.len().saturating_sub(min_match_len) {
        let end = std::cmp::min(i + 8, base.len());
        let window = &base[i..end];
        short_map.entry(window).or_default().push(i);
    }

    let mut target_pos = 0;

    while target_pos < target.len() {
        let mut best_match: Option<(usize, usize, usize)> = None;

        // For simple cases, check if we have a direct match from the beginning
        if target_pos == 0 && target.len() <= base.len() {
            // Check if the entire target matches the beginning of base
            let mut match_len = 0;
            while match_len < target.len() && base[match_len] == target[match_len] {
                match_len += 1;
            }
            if match_len >= min_match_len {
                best_match = Some((0, 0, match_len));
            }
        }

        // First try Rabin fingerprinting for longer matches
        if target_pos + RABIN_WINDOW_SIZE <= target.len() {
            let candidates = rabin_index.find_matches(target, target_pos);

            // Limit the number of candidates to check to avoid performance issues
            // with highly repetitive content
            let max_candidates = std::cmp::min(candidates.len(), 20);

            for &base_start in candidates.iter().take(max_candidates) {
                // Verify and extend the match
                let mut match_len = 0;
                let max_extend = std::cmp::min(base.len() - base_start, target.len() - target_pos);

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

                // Early exit if we found a very good match
                if match_len >= RABIN_WINDOW_SIZE * 2 {
                    break;
                }
            }
        }

        // If no good match found with Rabin, try short hash matches
        if best_match.is_none() {
            for window_size in (min_match_len..=std::cmp::min(8, target.len() - target_pos)).rev() {
                if target_pos + window_size <= target.len() {
                    let target_window = &target[target_pos..target_pos + window_size];

                    if let Some(base_positions) = short_map.get(target_window) {
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
                        break;
                    }
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

                // Check for Rabin fingerprint matches
                if target_pos + RABIN_WINDOW_SIZE <= target.len() {
                    let candidates = rabin_index.find_matches(target, target_pos);
                    if !candidates.is_empty() {
                        // Quick verification of at least one candidate
                        for &base_start in &candidates {
                            if base_start < base.len()
                                && target_pos < target.len()
                                && base[base_start] == target[target_pos]
                            {
                                found_match = true;
                                break;
                            }
                        }
                    }
                }

                // Check short hash matches if no Rabin match
                if !found_match {
                    for window_size in
                        (min_match_len..=std::cmp::min(8, target.len() - target_pos)).rev()
                    {
                        if target_pos + window_size <= target.len() {
                            let window = &target[target_pos..target_pos + window_size];
                            if short_map.contains_key(window) {
                                found_match = true;
                                break;
                            }
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

use pyo3::types::PyTuple;
use std::collections::VecDeque;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

/// Default window size for delta compression
const DEFAULT_PACK_DELTA_WINDOW_SIZE: usize = 10;

/// Maximum window size for delta compression
const MAX_PACK_DELTA_WINDOW_SIZE: usize = 250;

/// Memory limit for delta window (in bytes)
const MAX_WINDOW_MEMORY: usize = 256 * 1024 * 1024; // 256MB

/// Maximum depth of delta chains
const MAX_DELTA_DEPTH: usize = 50;

/// Calculate optimal window size based on object sizes and memory constraints
fn calculate_window_size(objects: &[(PyObject, ObjectData)], default_window: usize) -> usize {
    if objects.is_empty() {
        return default_window;
    }
    
    // Calculate average object size
    let total_size: usize = objects.iter().take(100).map(|(_, obj)| obj.raw_data.len()).sum();
    let avg_size = total_size / objects.len().min(100);
    
    // Calculate window size based on memory constraint
    let memory_based_window = if avg_size > 0 {
        MAX_WINDOW_MEMORY / avg_size
    } else {
        default_window
    };
    
    // Use minimum of memory-based, max allowed, and default * 2
    memory_based_window.min(MAX_PACK_DELTA_WINDOW_SIZE).min(default_window * 2).max(default_window)
}

/// Compute Git-style name hash for better delta candidate grouping
/// This is based on C Git's name_hash() function
fn compute_name_hash(path: &str) -> u32 {
    if path.is_empty() {
        return 0;
    }
    
    // Extract filename and extension for hashing
    let filename = path.split('/').last().unwrap_or(path);
    
    // Create hash input prioritizing extension and filename
    let hash_input = if let Some(dot_pos) = filename.rfind('.') {
        let (name, ext) = filename.split_at(dot_pos);
        format!("{}:{}", &ext[1..], name)  // ext:name format
    } else {
        filename.to_string()
    };
    
    // Simple hash function similar to C Git's approach
    let mut hash: u32 = 0;
    for byte in hash_input.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u32);
    }
    
    hash
}

/// Structure to hold object data for processing
struct ObjectData {
    sha: Vec<u8>,
    type_num: u8,
    raw_data: Vec<u8>,
    name_hash: u32,
    path: Option<String>,
    delta_depth: usize,
}

/// Sort objects for delta compression using Git's heuristic
fn sort_objects_for_delta(py: Python, objects: &PyObject) -> PyResult<Vec<(PyObject, ObjectData)>> {
    let mut magic = Vec::new();

    // Iterate through objects (can be ShaFile or (ShaFile, hint) tuples)
    let objects_list = objects.extract::<Bound<PyList>>(py)?;
    for item in objects_list.iter() {
        let (obj, hint) = if let Ok(tuple) = item.downcast::<PyTuple>() {
            // It's a tuple (object, hint)
            let obj = tuple.get_item(0)?;
            let hint = tuple.get_item(1)?;
            (obj, Some(hint))
        } else {
            // It's just an object
            (item, None)
        };

        // Extract type_num and path from hint if present
        let (type_num_hint, path) = if let Some(hint) = hint {
            if hint.is_none() {
                (None, None)
            } else if let Ok(hint_tuple) = hint.downcast::<PyTuple>() {
                let type_num = hint_tuple.get_item(0)?.extract::<Option<u8>>()?;
                let path = hint_tuple.get_item(1)?.extract::<Option<String>>()?;
                (type_num, path)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // Get object's properties
        let type_num = obj.getattr("type_num")?.extract::<u8>()?;
        let raw_length = obj.call_method0("raw_length")?.extract::<i64>()?;

        // Use hint type_num if available, otherwise use object's type_num
        let sort_type = type_num_hint.unwrap_or(type_num);

        // Compute name hash for Git-style sorting
        let name_hash = if let Some(ref path_str) = path {
            compute_name_hash(path_str)
        } else {
            0
        };

        magic.push((sort_type, name_hash, -raw_length, path, obj.unbind()));
    }

    // Sort by Git's magic heuristic: type_num, name_hash, size (descending)
    magic.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });

    // Prepare object data for efficient processing
    let mut result = Vec::new();
    for (_, name_hash, _, path, obj) in magic {
        // Get object data
        let py_obj = obj.bind(py);
        let type_num = py_obj.getattr("type_num")?.extract::<u8>()?;

        // Get SHA
        let sha_obj = py_obj.call_method0("sha")?;
        let sha_digest = sha_obj.call_method0("digest")?;
        let sha = sha_digest.extract::<&[u8]>()?.to_vec();

        // Get raw data
        let raw_chunks = py_obj.call_method0("as_raw_chunks")?;
        let raw_list = raw_chunks.downcast::<PyList>()?;
        let mut raw_data = Vec::new();
        for chunk in raw_list.iter() {
            let chunk_bytes = chunk.extract::<&[u8]>()?;
            raw_data.extend_from_slice(chunk_bytes);
        }

        result.push((
            obj,
            ObjectData {
                sha,
                type_num,
                raw_data,
                name_hash,
                path,
                delta_depth: 0,  // Base objects start with depth 0
            },
        ));
    }

    Ok(result)
}

/// Main deltify function implementation
#[pyfunction]
#[pyo3(signature = (objects, *, window_size=None, progress=None))]
fn deltify_pack_objects(
    py: Python,
    objects: PyObject,
    window_size: Option<usize>,
    progress: Option<PyObject>,
) -> PyResult<PyObject> {
    #[cfg(feature = "parallel")]
    {
        deltify_pack_objects_parallel(py, objects, window_size, progress)
    }
    #[cfg(not(feature = "parallel"))]
    {
        deltify_pack_objects_sequential(py, objects, window_size, progress)
    }
}

/// Sequential implementation for when parallel feature is disabled
fn deltify_pack_objects_sequential(
    py: Python,
    objects: PyObject,
    window_size: Option<usize>,
    progress: Option<PyObject>,
) -> PyResult<PyObject> {
    let window_size = window_size.unwrap_or(DEFAULT_PACK_DELTA_WINDOW_SIZE);

    // Convert objects to list with hints
    let objects_with_hints = PyList::empty(py);
    let objects_list = objects.extract::<Bound<PyList>>(py)?;

    for item in objects_list.iter() {
        if let Ok(_tuple) = item.downcast::<PyTuple>() {
            // Already a tuple
            objects_with_hints.append(item)?;
        } else {
            // Just an object - create tuple with hint
            let type_num = item.getattr("type_num")?;
            let none = py.None().into_bound(py);
            let hint = PyTuple::new(py, &[type_num, none])?;
            let tuple = PyTuple::new(py, &[item.to_owned(), hint.into_any()])?;
            objects_with_hints.append(tuple)?;
        }
    }

    // Sort objects
    let sorted_objects = sort_objects_for_delta(py, &objects_with_hints.into())?;

    // Calculate optimal window size
    let effective_window_size = calculate_window_size(&sorted_objects, window_size);

    // Generate deltas
    let results = PyList::empty(py);
    let mut possible_bases = VecDeque::<(Vec<u8>, u8, Vec<u8>, usize)>::new();  // Added depth
    let mut current_window_memory = 0usize;

    for (i, (_obj, obj_data)) in sorted_objects.iter().enumerate() {
        if let Some(ref progress_fn) = progress {
            if i % 1000 == 0 {
                let msg = format!("generating deltas: {}\r", i);
                progress_fn.call1(py, (PyBytes::new(py, msg.as_bytes()),))?;
            }
        }

        let mut winner = obj_data.raw_data.clone();
        let mut winner_len = winner.len();
        let mut winner_base = None;
        let mut winner_depth = 0;

        // Try delta compression against recent objects
        for (base_id, base_type_num, base_data, base_depth) in &possible_bases {
            if *base_type_num != obj_data.type_num {
                continue;
            }
            
            // Skip if using this base would exceed maximum delta depth
            if *base_depth >= MAX_DELTA_DEPTH {
                continue;
            }

            let delta = create_delta_internal(base_data, &obj_data.raw_data);

            if delta.len() < winner_len {
                winner_base = Some(base_id.clone());
                winner = delta;
                winner_len = winner.len();
                winner_depth = base_depth + 1;
            }
        }

        // Create delta chunks list
        let delta_chunks = PyList::empty(py);
        delta_chunks.append(PyBytes::new(py, &winner))?;

        // Import and create Python UnpackedObject
        let pack_module = py.import("dulwich.pack")?;
        let unpacked_class = pack_module.getattr("UnpackedObject")?;

        // Create UnpackedObject instance
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("sha", PyBytes::new(py, &obj_data.sha))?;
        if let Some(base) = winner_base {
            kwargs.set_item("delta_base", PyBytes::new(py, &base))?;
        } else {
            kwargs.set_item("delta_base", py.None())?;
        }
        kwargs.set_item("decomp_len", winner_len)?;
        kwargs.set_item("decomp_chunks", delta_chunks)?;

        let unpacked = unpacked_class.call((obj_data.type_num,), Some(&kwargs))?;
        results.append(unpacked)?;

        // Add this object as a potential base for future objects
        let obj_size = obj_data.raw_data.len();
        possible_bases.push_front((
            obj_data.sha.clone(),
            obj_data.type_num,
            obj_data.raw_data.clone(),
            winner_depth,  // Track the depth of this object
        ));
        current_window_memory += obj_size;

        // Maintain window size and memory constraints
        while possible_bases.len() > effective_window_size 
            || current_window_memory > MAX_WINDOW_MEMORY {
            if let Some((_, _, removed_data, _)) = possible_bases.pop_back() {
                current_window_memory = current_window_memory.saturating_sub(removed_data.len());
            }
        }
    }

    // Return iterator over results
    let iter_fn = py.import("builtins")?.getattr("iter")?;
    Ok(iter_fn.call1((results,))?.into())
}

#[cfg(feature = "parallel")]
/// Parallel implementation using rayon for better performance
fn deltify_pack_objects_parallel(
    py: Python,
    objects: PyObject,
    window_size: Option<usize>,
    progress: Option<PyObject>,
) -> PyResult<PyObject> {
    let window_size = window_size.unwrap_or(DEFAULT_PACK_DELTA_WINDOW_SIZE);

    // Convert objects to list with hints
    let objects_with_hints = PyList::empty(py);
    let objects_list = objects.extract::<Bound<PyList>>(py)?;

    for item in objects_list.iter() {
        if let Ok(_tuple) = item.downcast::<PyTuple>() {
            // Already a tuple
            objects_with_hints.append(item)?;
        } else {
            // Just an object - create tuple with hint
            let type_num = item.getattr("type_num")?;
            let none = py.None().into_bound(py);
            let hint = PyTuple::new(py, &[type_num, none])?;
            let tuple = PyTuple::new(py, &[item.to_owned(), hint.into_any()])?;
            objects_with_hints.append(tuple)?;
        }
    }

    // Sort objects and extract data
    let sorted_objects = sort_objects_for_delta(py, &objects_with_hints.into())?;
    let total_objects = sorted_objects.len();

    // For small counts, use sequential processing
    if total_objects < 50 {
        return deltify_pack_objects_sequential(py, objects, Some(window_size), progress);
    }

    // Calculate optimal window size
    let effective_window_size = calculate_window_size(&sorted_objects, window_size);

    // Extract object data for parallel processing
    let object_data: Vec<ObjectData> = sorted_objects.into_iter().map(|(_, data)| data).collect();

    // Process with parallel deltification
    let results = PyList::empty(py);
    let mut possible_bases = VecDeque::<(Vec<u8>, u8, Vec<u8>, usize)>::new();  // Added depth
    let mut current_window_memory = 0usize;

    for (i, obj_data) in object_data.iter().enumerate() {
        if let Some(ref progress_fn) = progress {
            if i % 1000 == 0 {
                let msg = format!("generating deltas: {}\r", i);
                progress_fn.call1(py, (PyBytes::new(py, msg.as_bytes()),))?;
            }
        }

        // Get snapshot of possible bases
        let bases_snapshot: Vec<_> = possible_bases.iter().cloned().collect();

        // Compute best delta in parallel if we have enough candidates
        let (winner, winner_len, winner_base, winner_depth) = if bases_snapshot.len() > 5 {
            // Worth parallelizing - release GIL and compute in parallel
            py.allow_threads(|| compute_best_delta_parallel(obj_data, &bases_snapshot))
        } else {
            // Not worth parallelizing
            compute_best_delta_sequential(obj_data, &bases_snapshot)
        };

        // Create delta chunks list
        let delta_chunks = PyList::empty(py);
        delta_chunks.append(PyBytes::new(py, &winner))?;

        // Import and create Python UnpackedObject
        let pack_module = py.import("dulwich.pack")?;
        let unpacked_class = pack_module.getattr("UnpackedObject")?;

        // Create UnpackedObject instance
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("sha", PyBytes::new(py, &obj_data.sha))?;
        if let Some(base) = winner_base {
            kwargs.set_item("delta_base", PyBytes::new(py, &base))?;
        } else {
            kwargs.set_item("delta_base", py.None())?;
        }
        kwargs.set_item("decomp_len", winner_len)?;
        kwargs.set_item("decomp_chunks", delta_chunks)?;

        let unpacked = unpacked_class.call((obj_data.type_num,), Some(&kwargs))?;
        results.append(unpacked)?;

        // Add this object as a potential base for future objects
        let obj_size = obj_data.raw_data.len();
        possible_bases.push_front((
            obj_data.sha.clone(),
            obj_data.type_num,
            obj_data.raw_data.clone(),
            winner_depth,  // Track the depth of this object
        ));
        current_window_memory += obj_size;

        // Maintain window size and memory constraints
        while possible_bases.len() > effective_window_size 
            || current_window_memory > MAX_WINDOW_MEMORY {
            if let Some((_, _, removed_data, _)) = possible_bases.pop_back() {
                current_window_memory = current_window_memory.saturating_sub(removed_data.len());
            }
        }
    }

    // Return iterator over results
    let iter_fn = py.import("builtins")?.getattr("iter")?;
    Ok(iter_fn.call1((results,))?.into())
}

/// Compute best delta sequentially
fn compute_best_delta_sequential(
    obj_data: &ObjectData,
    possible_bases: &[(Vec<u8>, u8, Vec<u8>, usize)],
) -> (Vec<u8>, usize, Option<Vec<u8>>, usize) {
    let mut winner = obj_data.raw_data.clone();
    let mut winner_len = winner.len();
    let mut winner_base = None;
    let mut winner_depth = 0;

    // Try delta compression against recent objects
    for (base_id, base_type_num, base_data, base_depth) in possible_bases {
        if *base_type_num != obj_data.type_num {
            continue;
        }
        
        // Skip if using this base would exceed maximum delta depth
        if *base_depth >= MAX_DELTA_DEPTH {
            continue;
        }

        let delta = create_delta_internal(base_data, &obj_data.raw_data);

        if delta.len() < winner_len {
            winner_base = Some(base_id.clone());
            winner = delta;
            winner_len = winner.len();
            winner_depth = base_depth + 1;
        }
    }

    (winner, winner_len, winner_base, winner_depth)
}

#[cfg(feature = "parallel")]
/// Compute best delta using parallel processing
fn compute_best_delta_parallel(
    obj_data: &ObjectData,
    possible_bases: &[(Vec<u8>, u8, Vec<u8>, usize)],
) -> (Vec<u8>, usize, Option<Vec<u8>>, usize) {
    // Filter bases by type and depth first to avoid unnecessary work
    let compatible_bases: Vec<_> = possible_bases
        .iter()
        .filter(|(_, base_type_num, _, base_depth)| {
            *base_type_num == obj_data.type_num && *base_depth < MAX_DELTA_DEPTH
        })
        .collect();

    if compatible_bases.is_empty() {
        return (obj_data.raw_data.clone(), obj_data.raw_data.len(), None, 0);
    }

    // Compute deltas in parallel
    let results: Vec<_> = compatible_bases
        .par_iter()
        .map(|(base_id, _, base_data, base_depth)| {
            let delta = create_delta_internal(base_data, &obj_data.raw_data);
            (base_id.clone(), delta.len(), delta, base_depth + 1)
        })
        .collect();

    // Find the best result
    let mut best_size = obj_data.raw_data.len();
    let mut best_base = None;
    let mut best_delta = obj_data.raw_data.clone();
    let mut best_depth = 0;

    for (base_id, delta_len, delta, depth) in results {
        if delta_len < best_size {
            best_size = delta_len;
            best_base = Some(base_id);
            best_delta = delta;
            best_depth = depth;
        }
    }

    (best_delta, best_size, best_base, best_depth)
}

#[pymodule]
fn _pack(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bisect_find_sha, m)?)?;
    m.add_function(wrap_pyfunction!(apply_delta, m)?)?;
    m.add_function(wrap_pyfunction!(create_delta, m)?)?;
    m.add_function(wrap_pyfunction!(deltify_pack_objects, m)?)?;
    Ok(())
}
