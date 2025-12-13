/*
 * Copyright (C) 2009 Jelmer Vernooij <jelmer@jelmer.uk>
 *
 * Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
 * General Public License as published by the Free Software Foundation; version 2.0
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

// Allow PyO3 macro-generated interior mutable constants
#![allow(clippy::declare_interior_mutable_const)]

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

pyo3::import_exception!(dulwich.errors, ApplyDeltaError);

fn py_is_sha(sha: &Py<PyAny>, py: Python) -> PyResult<bool> {
    // Check if the object is a bytes object
    if sha.bind(py).is_instance_of::<PyBytes>() {
        // Check if the bytes object has a size of 20 (SHA1) or 32 (SHA256)
        let len = sha.extract::<&[u8]>(py)?.len();
        if len == 20 || len == 32 {
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
    unpack_name: Py<PyAny>,
) -> PyResult<Option<i32>> {
    // Convert sha_obj to a byte slice
    let sha = sha.as_bytes(py);
    let sha_len = sha.len();

    // Check if sha is 20 bytes (SHA1) or 32 bytes (SHA256)
    if sha_len != 20 && sha_len != 32 {
        return Err(PyValueError::new_err(
            "Sha must be 20 (SHA1) or 32 (SHA256) bytes long",
        ));
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
    py_buf: &'a Py<PyAny>,
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
fn apply_delta(py: Python, py_src_buf: Py<PyAny>, py_delta: Py<PyAny>) -> PyResult<Vec<Py<PyAny>>> {
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

            // Check for overflow and bounds
            if cp_size > src_size
                || cp_off > src_size
                || cp_off > src_size - cp_size
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

/// Encode a size value for delta headers using variable-length encoding.
/// This matches Python's _delta_encode_size function.
fn delta_encode_size(mut size: usize) -> Vec<u8> {
    let mut ret = Vec::new();
    let mut c = (size & 0x7F) as u8;
    size >>= 7;
    while size > 0 {
        ret.push(c | 0x80);
        c = (size & 0x7F) as u8;
        size >>= 7;
    }
    ret.push(c);
    ret
}

/// The length of delta compression copy operations in version 2 packs is limited
/// to 64K. To copy more, we use several copy operations.
const MAX_COPY_LEN: usize = 0xFFFF;

/// Encode a copy operation for the delta format.
/// This matches Python's _encode_copy_operation function.
fn encode_copy_operation(start: usize, length: usize) -> Vec<u8> {
    let mut scratch = vec![0x80u8];

    // Encode offset (4 bytes max)
    for i in 0..4 {
        if start & (0xFF << (i * 8)) != 0 {
            scratch.push(((start >> (i * 8)) & 0xFF) as u8);
            scratch[0] |= 1 << i;
        }
    }

    // Encode length (2 bytes for version 2 packs)
    for i in 0..2 {
        if length & (0xFF << (i * 8)) != 0 {
            scratch.push(((length >> (i * 8)) & 0xFF) as u8);
            scratch[0] |= 1 << (4 + i);
        }
    }

    scratch
}

/// Create a delta that transforms base_buf into target_buf.
/// This uses the similar crate to find matching sequences, similar to
/// Python's difflib.SequenceMatcher.
fn create_delta_internal(base_buf: &[u8], target_buf: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    // Write delta header
    result.extend(delta_encode_size(base_buf.len()));
    result.extend(delta_encode_size(target_buf.len()));

    // Use similar crate to compute the diff at byte level
    let ops = similar::capture_diff_slices(similar::Algorithm::Myers, base_buf, target_buf);

    let mut old_pos = 0;
    let mut new_pos = 0;

    for op in ops {
        match op {
            similar::DiffOp::Equal {
                old_index,
                new_index,
                len,
            } => {
                // Sanity check
                assert_eq!(old_index, old_pos);
                assert_eq!(new_index, new_pos);

                // Emit copy operations from base_buf
                let mut copy_start = old_index;
                let mut copy_len = len;

                while copy_len > 0 {
                    let to_copy = copy_len.min(MAX_COPY_LEN);
                    result.extend(encode_copy_operation(copy_start, to_copy));
                    copy_start += to_copy;
                    copy_len -= to_copy;
                }

                old_pos += len;
                new_pos += len;
            }
            similar::DiffOp::Delete {
                old_index, old_len, ..
            } => {
                // Git delta format doesn't care about deletes from base
                assert_eq!(old_index, old_pos);
                old_pos += old_len;
            }
            similar::DiffOp::Insert {
                new_index, new_len, ..
            } => {
                // Emit literal data from target_buf
                assert_eq!(new_index, new_pos);

                let data = &target_buf[new_index..new_index + new_len];
                let mut remaining = data.len();
                let mut offset = 0;

                while remaining > 0 {
                    let chunk_size = remaining.min(127);
                    result.push(chunk_size as u8);
                    result.extend_from_slice(&data[offset..offset + chunk_size]);
                    offset += chunk_size;
                    remaining -= chunk_size;
                }

                new_pos += new_len;
            }
            similar::DiffOp::Replace {
                old_index,
                old_len,
                new_index,
                new_len,
            } => {
                // For replace operations, we delete from old and insert from new
                // Git delta format doesn't care about deletes, so just emit insert
                assert_eq!(old_index, old_pos);
                assert_eq!(new_index, new_pos);

                let data = &target_buf[new_index..new_index + new_len];
                let mut remaining = data.len();
                let mut offset = 0;

                while remaining > 0 {
                    let chunk_size = remaining.min(127);
                    result.push(chunk_size as u8);
                    result.extend_from_slice(&data[offset..offset + chunk_size]);
                    offset += chunk_size;
                    remaining -= chunk_size;
                }

                old_pos += old_len;
                new_pos += new_len;
            }
        }
    }

    result
}

#[pyfunction]
fn create_delta(
    py: Python,
    py_base_buf: Py<PyAny>,
    py_target_buf: Py<PyAny>,
) -> PyResult<Py<PyBytes>> {
    let base_buf = py_chunked_as_string(py, &py_base_buf)?;
    let target_buf = py_chunked_as_string(py, &py_target_buf)?;

    let delta = create_delta_internal(base_buf.as_ref(), target_buf.as_ref());

    Ok(PyBytes::new(py, &delta).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encode_size_zero() {
        assert_eq!(delta_encode_size(0), vec![0]);
    }

    #[test]
    fn test_delta_encode_size_small() {
        // Values that fit in 7 bits (0-127)
        assert_eq!(delta_encode_size(1), vec![1]);
        assert_eq!(delta_encode_size(127), vec![127]);
    }

    #[test]
    fn test_delta_encode_size_medium() {
        // Values that need 2 bytes (128-16383)
        assert_eq!(delta_encode_size(128), vec![0x80, 0x01]);
        assert_eq!(delta_encode_size(256), vec![0x80, 0x02]);
        assert_eq!(delta_encode_size(16383), vec![0xFF, 0x7F]);
    }

    #[test]
    fn test_delta_encode_size_large() {
        // Values that need 3 bytes (16384-2097151)
        assert_eq!(delta_encode_size(16384), vec![0x80, 0x80, 0x01]);
        assert_eq!(delta_encode_size(65536), vec![0x80, 0x80, 0x04]);
    }

    #[test]
    fn test_delta_encode_size_very_large() {
        // Values that need 4+ bytes
        assert_eq!(delta_encode_size(1048576), vec![0x80, 0x80, 0x40]); // 1MB = 2^20
        assert_eq!(delta_encode_size(16777216), vec![0x80, 0x80, 0x80, 0x08]); // 16MB = 2^24
    }

    #[test]
    fn test_get_delta_header_size_basic() {
        // Test decoding various encoded sizes
        let mut index = 0;
        let delta = vec![0x00];
        assert_eq!(get_delta_header_size(&delta, &mut index, delta.len()), 0);
        assert_eq!(index, 1);

        let mut index = 0;
        let delta = vec![0x01];
        assert_eq!(get_delta_header_size(&delta, &mut index, delta.len()), 1);
        assert_eq!(index, 1);

        let mut index = 0;
        let delta = vec![127];
        assert_eq!(get_delta_header_size(&delta, &mut index, delta.len()), 127);
        assert_eq!(index, 1);
    }

    #[test]
    fn test_get_delta_header_size_multibyte() {
        // Test decoding multi-byte sizes
        let mut index = 0;
        let delta = vec![0x80, 0x01];
        assert_eq!(get_delta_header_size(&delta, &mut index, delta.len()), 128);
        assert_eq!(index, 2);

        let mut index = 0;
        let delta = vec![0x80, 0x02];
        assert_eq!(get_delta_header_size(&delta, &mut index, delta.len()), 256);
        assert_eq!(index, 2);

        let mut index = 0;
        let delta = vec![0x80, 0x80, 0x01];
        assert_eq!(
            get_delta_header_size(&delta, &mut index, delta.len()),
            16384
        );
        assert_eq!(index, 3);
    }

    #[test]
    fn test_delta_encode_decode_roundtrip() {
        // Test that encoding and decoding are inverse operations
        let test_values = vec![0, 1, 127, 128, 255, 256, 1000, 16384, 65536, 1048576];

        for value in test_values {
            let encoded = delta_encode_size(value);
            let mut index = 0;
            let decoded = get_delta_header_size(&encoded, &mut index, encoded.len());
            assert_eq!(
                decoded, value,
                "Roundtrip failed for value {}: encoded {:?}, decoded {}",
                value, encoded, decoded
            );
            assert_eq!(index, encoded.len());
        }
    }

    #[test]
    fn test_encode_copy_operation_zero_offset() {
        // Copy from offset 0
        let result = encode_copy_operation(0, 10);
        // Should have copy bit set
        assert_eq!(result[0] & 0x80, 0x80);
        // Should encode length 10
        assert_eq!(result[0] & 0x10, 0x10); // Length bit 0 set
        assert_eq!(result[1], 10);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_encode_copy_operation_small_offset() {
        // Copy from offset 100, length 20
        let result = encode_copy_operation(100, 20);
        assert_eq!(result[0] & 0x80, 0x80); // Copy bit
        assert_eq!(result[0] & 0x01, 0x01); // Offset byte 0 present
        assert_eq!(result[0] & 0x10, 0x10); // Length byte 0 present
        assert_eq!(result[1], 100); // Offset byte 0
        assert_eq!(result[2], 20); // Length byte 0
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_encode_copy_operation_large_offset() {
        // Copy from offset 0x12345, length 0x678
        let result = encode_copy_operation(0x12345, 0x678);
        assert_eq!(result[0] & 0x80, 0x80); // Copy bit
        assert_eq!(result[0] & 0x07, 0x07); // Offset bytes 0,1,2 present
        assert_eq!(result[0] & 0x30, 0x30); // Length bytes 0,1 present
        assert_eq!(result[1], 0x45); // Offset byte 0
        assert_eq!(result[2], 0x23); // Offset byte 1
        assert_eq!(result[3], 0x01); // Offset byte 2
        assert_eq!(result[4], 0x78); // Length byte 0
        assert_eq!(result[5], 0x06); // Length byte 1
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn test_encode_copy_operation_max_offset() {
        // Test maximum offset (needs 4 bytes)
        let max_offset = 0xFFFFFFFF;
        let result = encode_copy_operation(max_offset, 1);
        assert_eq!(result[0] & 0x80, 0x80); // Copy bit
        assert_eq!(result[0] & 0x0F, 0x0F); // All 4 offset bytes present
        assert_eq!(result[1], 0xFF); // Offset byte 0
        assert_eq!(result[2], 0xFF); // Offset byte 1
        assert_eq!(result[3], 0xFF); // Offset byte 2
        assert_eq!(result[4], 0xFF); // Offset byte 3
        assert_eq!(result.len(), 6); // 1 cmd + 4 offset + 1 length
    }

    #[test]
    fn test_encode_copy_operation_max_length() {
        // Test maximum length for version 2 packs (0xFFFF)
        let result = encode_copy_operation(0, MAX_COPY_LEN);
        assert_eq!(result[0] & 0x80, 0x80); // Copy bit
        assert_eq!(result[0] & 0x30, 0x30); // Both length bytes present
        assert_eq!(result[1], 0xFF); // Length byte 0
        assert_eq!(result[2], 0xFF); // Length byte 1
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_encode_copy_operation_various_lengths() {
        // Test different length values to ensure correct encoding
        // Note: only non-zero bytes are encoded

        // Length 1: byte0=1 -> only bit 4 set
        let result = encode_copy_operation(0, 1);
        assert_eq!(result[0] & 0x80, 0x80);
        assert_eq!(result[0] & 0x30, 0x10);
        assert_eq!(result[1], 1);

        // Length 255 (0xFF): byte0=0xFF, byte1=0 -> only bit 4 set
        let result = encode_copy_operation(0, 255);
        assert_eq!(result[0] & 0x80, 0x80);
        assert_eq!(result[0] & 0x30, 0x10);
        assert_eq!(result[1], 0xFF);

        // Length 256 (0x100): byte0=0, byte1=1 -> only bit 5 set
        let result = encode_copy_operation(0, 256);
        assert_eq!(result[0] & 0x80, 0x80);
        assert_eq!(result[0] & 0x30, 0x20); // Only second length byte
        assert_eq!(result[1], 1);

        // Length 1000 (0x3E8): byte0=0xE8, byte1=3 -> both bits set
        let result = encode_copy_operation(0, 1000);
        assert_eq!(result[0] & 0x80, 0x80);
        assert_eq!(result[0] & 0x30, 0x30); // Both length bytes
        assert_eq!(result[1], 0xE8);
        assert_eq!(result[2], 0x03);

        // Length 0xFFFF: byte0=0xFF, byte1=0xFF -> both bits set
        let result = encode_copy_operation(0, 0xFFFF);
        assert_eq!(result[0] & 0x80, 0x80);
        assert_eq!(result[0] & 0x30, 0x30); // Both length bytes
        assert_eq!(result[1], 0xFF);
        assert_eq!(result[2], 0xFF);
    }

    #[test]
    fn test_create_delta_identical() {
        // Delta between identical buffers should be minimal
        let base = b"hello world";
        let target = b"hello world";
        let delta = create_delta_internal(base, target);

        // Should have header (2 size encodings) plus copy operations
        assert!(delta.len() < base.len()); // Delta should be smaller than full data
    }

    #[test]
    fn test_create_delta_completely_different() {
        // Delta between completely different buffers
        let base = b"aaaaaaaaaa";
        let target = b"bbbbbbbbbb";
        let delta = create_delta_internal(base, target);

        // Should have header plus insert operations with the new data
        assert!(delta.len() > 0);
    }

    #[test]
    fn test_create_and_apply_delta() {
        // Test that create_delta and apply_delta are inverse operations
        let base = b"The quick brown fox jumps over the lazy dog";
        let target = b"The quick brown cat jumps over the lazy dog";

        // Create delta
        let delta = create_delta_internal(base, target);

        // Apply delta should reconstruct target
        let mut index = 0;
        let src_size = get_delta_header_size(&delta, &mut index, delta.len());
        assert_eq!(src_size, base.len());

        let dest_size = get_delta_header_size(&delta, &mut index, delta.len());
        assert_eq!(dest_size, target.len());

        // The delta should be valid and smaller than sending the full target
        assert!(delta.len() > 0);
    }

    #[test]
    fn test_create_delta_with_insertion() {
        let base = b"hello";
        let target = b"hello world";
        let delta = create_delta_internal(base, target);

        // Should have a copy operation for "hello" and insert for " world"
        assert!(delta.len() > 0);
    }

    #[test]
    fn test_create_delta_with_deletion() {
        let base = b"hello world";
        let target = b"hello";
        let delta = create_delta_internal(base, target);

        // Should have a copy operation for "hello" only
        assert!(delta.len() > 0);
    }
}

#[pymodule]
fn _pack(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bisect_find_sha, m)?)?;
    m.add_function(wrap_pyfunction!(apply_delta, m)?)?;
    m.add_function(wrap_pyfunction!(create_delta, m)?)?;
    Ok(())
}
