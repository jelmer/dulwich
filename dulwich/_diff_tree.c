/*
 * Copyright (C) 2010 Google, Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2
 * of the License or (at your option) a later version of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

#include <Python.h>
#include <sys/stat.h>

static PyObject *py_is_tree(PyObject *self, PyObject *args)
{
	PyObject *entry, *mode, *result;
	long lmode;

	if (!PyArg_ParseTuple(args, "O", &entry))
		return NULL;

	mode = PyObject_GetAttrString(entry, "mode");
	if (!mode)
		return NULL;

	if (mode == Py_None) {
		result = Py_False;
	} else {
		lmode = PyInt_AsLong(mode);
		if (lmode == -1 && PyErr_Occurred()) {
			Py_DECREF(mode);
			return NULL;
		}
		result = PyBool_FromLong(S_ISDIR((mode_t)lmode));
	}
	Py_INCREF(result);
	Py_DECREF(mode);
	return result;
}

static PyMethodDef py_diff_tree_methods[] = {
	{ "_is_tree", (PyCFunction)py_is_tree, METH_VARARGS, NULL },
	{ NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC
init_diff_tree(void)
{
	PyObject *m, *objects_mod, *diff_mod;
	m = Py_InitModule("_diff_tree", py_diff_tree_methods);
	if (!m)
		return;
}
