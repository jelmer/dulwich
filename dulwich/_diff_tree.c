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

#if (PY_VERSION_HEX < 0x02050000)
typedef int Py_ssize_t;
#endif

#if (PY_VERSION_HEX < 0x02060000)
#define Py_SIZE(x) Py_Size(x)
#endif

static PyObject *tree_entry_cls, *null_entry;

/**
 * Free an array of PyObject pointers, decrementing any references.
 */
static void free_objects(PyObject **objs, Py_ssize_t n) {
	Py_ssize_t i;
	for (i = 0; i < n; i++)
		Py_XDECREF(objs[i]);
	PyMem_Free(objs);
}

/**
 * Get the entries of a tree, prepending the given path.
 *
 * :param path: The path to prepend, without trailing slashes.
 * :param path_len: The length of path.
 * :param tree: The Tree object to iterate.
 * :param n: Set to the length of result.
 * :return: A (C) array of PyObject pointers to TreeEntry objects for each path
 *     in tree.
 */
static PyObject **tree_entries(char *path, Py_ssize_t path_len, PyObject *tree,
		Py_ssize_t *n) {
	PyObject *iteritems, *items, **result = NULL;
	PyObject *old_entry, *name, *sha;
	Py_ssize_t i = 0, name_len, new_path_len;
	char *new_path;

	if (tree == Py_None) {
		*n = 0;
		result = PyMem_New(PyObject*, 0);
		if (!result) {
			PyErr_SetNone(PyExc_MemoryError);
			return NULL;
		}
		return result;
	}

	iteritems = PyObject_GetAttrString(tree, "iteritems");
	if (!iteritems)
		return NULL;
	items = PyObject_CallFunctionObjArgs(iteritems, Py_True, NULL);
	Py_DECREF(iteritems);
	if (!items) {
		return NULL;
	}
	/* The C implementation of iteritems returns a list, so depend on that. */
	if (!PyList_Check(items)) {
		PyErr_SetString(PyExc_TypeError,
			"Tree.iteritems() did not return a list");
		return NULL;
	}

	*n = PyList_Size(items);
	result = PyMem_New(PyObject*, *n);
	if (!result) {
		PyErr_SetNone(PyExc_MemoryError);
		goto error;
	}
	for (i = 0; i < *n; i++) {
		old_entry = PyList_GetItem(items, i);
		if (!old_entry)
			goto error;
		sha = PyTuple_GetItem(old_entry, 2);
		if (!sha)
			goto error;
		name = PyTuple_GET_ITEM(old_entry, 0);
		name_len = PyString_Size(name);
		if (PyErr_Occurred())
			goto error;

		new_path_len = name_len;
		if (path_len)
			new_path_len += path_len + 1;
		new_path = PyMem_Malloc(new_path_len);
		if (!new_path) {
			PyErr_SetNone(PyExc_MemoryError);
			goto error;
		}
		if (path_len) {
			memcpy(new_path, path, path_len);
			new_path[path_len] = '/';
			memcpy(new_path + path_len + 1, PyString_AS_STRING(name), name_len);
		} else {
			memcpy(new_path, PyString_AS_STRING(name), name_len);
		}

		result[i] = PyObject_CallFunction(tree_entry_cls, "s#OO", new_path,
			new_path_len, PyTuple_GET_ITEM(old_entry, 1), sha);
		PyMem_Free(new_path);
		if (!result[i]) {
			goto error;
		}
	}
	Py_DECREF(items);
	return result;

error:
	free_objects(result, i);
	Py_DECREF(items);
	return NULL;
}

/**
 * Use strcmp to compare the paths of two TreeEntry objects.
 */
static int entry_path_cmp(PyObject *entry1, PyObject *entry2) {
	PyObject *path1 = NULL, *path2 = NULL;
	int result;

	path1 = PyObject_GetAttrString(entry1, "path");
	if (!path1)
		goto done;
	if (!PyString_Check(path1)) {
		PyErr_SetString(PyExc_TypeError, "path is not a string");
		goto done;
	}

	path2 = PyObject_GetAttrString(entry2, "path");
	if (!path2)
		goto done;
	if (!PyString_Check(path2)) {
		PyErr_SetString(PyExc_TypeError, "path is not a string");
		goto done;
	}

	result = strcmp(PyString_AS_STRING(path1), PyString_AS_STRING(path2));

done:
	Py_XDECREF(path1);
	Py_XDECREF(path2);
	return result;
}

static PyObject *py_merge_entries(PyObject *self, PyObject *args)
{
	PyObject *path, *tree1, *tree2, **entries1 = NULL, **entries2 = NULL;
	PyObject *e1, *e2, *pair, *result = NULL;
	Py_ssize_t path_len, n1 = 0, n2 = 0, i1 = 0, i2 = 0;
	char *path_str;
	int cmp;

	if (!PyArg_ParseTuple(args, "OOO", &path, &tree1, &tree2))
		return NULL;

	path_str = PyString_AsString(path);
	if (!path_str) {
		PyErr_SetString(PyExc_TypeError, "path is not a string");
		return NULL;
	}
	path_len = PyString_GET_SIZE(path);

	entries1 = tree_entries(path_str, path_len, tree1, &n1);
	if (!entries1)
		goto error;
	entries2 = tree_entries(path_str, path_len, tree2, &n2);
	if (!entries2)
		goto error;

	result = PyList_New(n1 + n2);
	if (!result)
		goto error;
	/* PyList_New sets the len of the list, not its allocated size, so we
	 * need to trim it to the size we actually use. */
	Py_SIZE(result) = 0;

	while (i1 < n1 && i2 < n2) {
		cmp = entry_path_cmp(entries1[i1], entries2[i2]);
		if (PyErr_Occurred())
			goto error;
		if (!cmp) {
			e1 = entries1[i1++];
			e2 = entries2[i2++];
		} else if (cmp < 0) {
			e1 = entries1[i1++];
			e2 = null_entry;
		} else {
			e1 = null_entry;
			e2 = entries2[i2++];
		}
		pair = PyTuple_Pack(2, e1, e2);
		if (!pair)
			goto error;
		PyList_SET_ITEM(result, Py_SIZE(result)++, pair);
	}

	while (i1 < n1) {
		pair = PyTuple_Pack(2, entries1[i1++], null_entry);
		if (!pair)
			goto error;
		PyList_SET_ITEM(result, Py_SIZE(result)++, pair);
	}
	while (i2 < n2) {
		pair = PyTuple_Pack(2, null_entry, entries2[i2++]);
		if (!pair)
			goto error;
		PyList_SET_ITEM(result, Py_SIZE(result)++, pair);
	}
	goto done;

error:
	Py_XDECREF(result);
	result = NULL;

done:
	free_objects(entries1, n1);
	free_objects(entries2, n2);
	return result;
}

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
	{ "_merge_entries", (PyCFunction)py_merge_entries, METH_VARARGS, NULL },
	{ NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC
init_diff_tree(void)
{
	PyObject *m, *objects_mod, *diff_mod;
	m = Py_InitModule("_diff_tree", py_diff_tree_methods);
	if (!m)
		return;

	objects_mod = PyImport_ImportModule("dulwich.objects");
	if (!objects_mod)
		return;

	tree_entry_cls = PyObject_GetAttrString(objects_mod, "TreeEntry");
	Py_DECREF(objects_mod);
	if (!tree_entry_cls)
		return;

	diff_mod = PyImport_ImportModule("dulwich.diff");
	if (!diff_mod)
		return;
	null_entry = PyObject_GetAttrString(diff_mod, "_NULL_ENTRY");
	Py_DECREF(diff_mod);
	if (!null_entry)
		return;
}
