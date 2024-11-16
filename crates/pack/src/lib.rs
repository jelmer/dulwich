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

#[pymodule]
fn _pack(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bisect_find_sha, m)?)?;
    m.add_function(wrap_pyfunction!(apply_delta, m)?)?;
    Ok(())
}
