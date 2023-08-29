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

use memchr::memchr;
use std::borrow::Cow;

use pyo3::exceptions::PyTypeError;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

import_exception!(dulwich.errors, ObjectFormatException);

const S_IFDIR: u32 = 0o40000;

fn bytehex(byte: u8) -> u8 {
    match byte {
        0..=9 => byte + b'0',
        10..=15 => byte - 10 + b'a',
        _ => unreachable!(),
    }
}

fn sha_to_pyhex(py: Python, sha: &[u8]) -> PyResult<PyObject> {
    let mut hexsha = Vec::new();
    for c in sha {
        hexsha.push(bytehex((c & 0xF0) >> 4));
        hexsha.push(bytehex(c & 0x0F));
    }

    Ok(PyBytes::new(py, hexsha.as_slice()).into())
}

#[pyfunction]
fn parse_tree(
    py: Python,
    mut text: &[u8],
    strict: Option<bool>,
) -> PyResult<Vec<(PyObject, u32, PyObject)>> {
    let mut entries = Vec::new();
    let strict = strict.unwrap_or(false);
    while !text.is_empty() {
        let mode_end = match memchr(b' ', text) {
            Some(e) => e,
            None => {
                return Err(ObjectFormatException::new_err((
                    "Missing terminator for mode",
                )));
            }
        };
        let text_str = String::from_utf8_lossy(&text[..mode_end]).to_string();
        let mode = match u32::from_str_radix(text_str.as_str(), 8) {
            Ok(m) => m,
            Err(e) => {
                return Err(ObjectFormatException::new_err((format!(
                    "invalid mode: {}",
                    e
                ),)));
            }
        };
        if strict && text[0] == b'0' {
            return Err(ObjectFormatException::new_err((
                "Illegal leading zero on mode",
            )));
        }
        text = &text[mode_end + 1..];
        let namelen = match memchr(b'\0', text) {
            Some(nl) => nl,
            None => {
                return Err(ObjectFormatException::new_err(("Missing trailing \\0",)));
            }
        };
        let name = &text[..namelen];
        if namelen + 20 >= text.len() {
            return Err(ObjectFormatException::new_err(("SHA truncated",)));
        }
        text = &text[namelen + 1..];
        let sha = &text[..20];
        entries.push((
            PyBytes::new(py, name).to_object(py),
            mode,
            sha_to_pyhex(py, sha)?,
        ));
        text = &text[20..];
    }
    Ok(entries)
}

fn name_with_suffix(mode: u32, name: &[u8]) -> Cow<[u8]> {
    if mode & S_IFDIR != 0 {
        let mut v = name.to_vec();
        v.push(b'/');
        v.into()
    } else {
        name.into()
    }
}

/// Iterate over a tree entries dictionary.
///
/// # Arguments
///
///   name_order: If True, iterate entries in order of their name. If
///        False, iterate entries in tree order, that is, treat subtree entries as
///        having '/' appended.
///      entries: Dictionary mapping names to (mode, sha) tuples
///
/// # Returns: Iterator over (name, mode, hexsha)
#[pyfunction]
fn sorted_tree_items(py: Python, entries: &PyDict, name_order: bool) -> PyResult<Vec<PyObject>> {
    let mut qsort_entries = Vec::new();
    for (name, e) in entries.iter() {
        let (mode, sha): (u32, Vec<u8>) = match e.extract() {
            Ok(o) => o,
            Err(e) => {
                return Err(PyTypeError::new_err((format!("invalid type: {}", e),)));
            }
        };
        qsort_entries.push((name.extract::<Vec<u8>>().unwrap(), mode, sha));
    }
    if name_order {
        qsort_entries.sort_by(|a, b| a.0.cmp(&b.0));
    } else {
        qsort_entries.sort_by(|a, b| {
            name_with_suffix(a.1, a.0.as_slice()).cmp(&name_with_suffix(b.1, b.0.as_slice()))
        });
    }
    let objectsm = py.import("dulwich.objects")?;
    let tree_entry_cls = objectsm.getattr("TreeEntry")?;
    qsort_entries
        .into_iter()
        .map(|(name, mode, hexsha)| -> PyResult<PyObject> {
            Ok(tree_entry_cls
                .call1((
                    PyBytes::new(py, name.as_slice()).to_object(py),
                    mode,
                    PyBytes::new(py, hexsha.as_slice()).to_object(py),
                ))?
                .to_object(py))
        })
        .collect::<PyResult<Vec<PyObject>>>()
}

#[pymodule]
fn _objects(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sorted_tree_items, m)?)?;
    m.add_function(wrap_pyfunction!(parse_tree, m)?)?;
    Ok(())
}
