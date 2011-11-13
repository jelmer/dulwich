/*
 * Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>
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
#include <stdlib.h>
#include <sys/stat.h>

#if (PY_VERSION_HEX < 0x02050000)
typedef int Py_ssize_t;
#endif

#if defined(__MINGW32_VERSION) || defined(__APPLE__)
size_t rep_strnlen(char *text, size_t maxlen);
size_t rep_strnlen(char *text, size_t maxlen)
{
	const char *last = memchr(text, '\0', maxlen);
	return last ? (size_t) (last - text) : maxlen;
}
#define strnlen rep_strnlen
#endif

#define bytehex(x) (((x)<0xa)?('0'+(x)):('a'-0xa+(x)))

static PyObject *tree_entry_cls;
static PyObject *object_format_exception_cls;

static PyObject* ParseTreeIter_iter(PyObject *self);
static PyObject* ParseTreeIter_iternext(PyObject *self);

typedef struct {
	PyObject_HEAD
	PyObject *py_text;
	Py_ssize_t len;
	int strict;
	char *text;
	char *start;
	char *end;
} ParseTreeIter_state;

static PyTypeObject _objects_ParseTreeIterType = {
    PyObject_HEAD_INIT(NULL)
    0,                                /* ob_size */
	"_objects.ParseTreeIter",         /* tp_name */
	sizeof(ParseTreeIter_state),      /* tp_basicsize */
	0,                                /* tp_itemsize */
	0,                                /* tp_dealloc */
	0,                                /* tp_print */
	0,                                /* tp_getattr */
	0,                                /* tp_setattr */
	0,                                /* tp_compare */
	0,                                /* tp_repr */
	0,                                /* tp_as_number */
	0,                                /* tp_as_sequence */
	0,                                /* tp_as_mapping */
	0,                                /* tp_hash */
	0,                                /* tp_call */
	0,                                /* tp_str */
	0,                                /* tp_getattro */
	0,                                /* tp_setattro */
	0,                                /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
	/* tp_flags: Py_TPFLAGS_HAVE_ITER tells python to
	   use tp_iter and tp_iternext fields. */
	"iterator object for parse_tree", /* tp_doc */
	0,                                /* tp_traverse */
	0,                                /* tp_clear */
	0,                                /* tp_richcompare */
	0,                                /* tp_weaklistoffset */
	ParseTreeIter_iter,               /* tp_iter: __iter__() method */
	ParseTreeIter_iternext            /* tp_iternext: next() method */
};

static PyObject *sha_to_pyhex(const unsigned char *sha)
{
	char hexsha[41];
	int i;
	for (i = 0; i < 20; i++) {
		hexsha[i*2] = bytehex((sha[i] & 0xF0) >> 4);
		hexsha[i*2+1] = bytehex(sha[i] & 0x0F);
	}

	return PyString_FromStringAndSize(hexsha, 40);
}

static PyObject* ParseTreeIter_iter(PyObject *self) {
	Py_INCREF(self);
	return self;
}

static PyObject* ParseTreeIter_iternext(PyObject *self) {
	ParseTreeIter_state *state = (ParseTreeIter_state*)self;
	PyObject *item, *name;
	long mode;
	int namelen;

	if (state->text < state->end) {
		if (state->strict && state->text[0] == '0') {
			PyErr_SetString(object_format_exception_cls,
			                "Illegal leading zero on mode");
			Py_DECREF(state->py_text);
			return NULL;
		}

		mode = strtol(state->text, &(state->text), 8);

		if (*(state->text) != ' ') {
			//printf("Expected space: %s\n", state->text);
			PyErr_SetString(PyExc_ValueError, "Expected space");
			Py_DECREF(state->py_text);
			return NULL;
		}

		state->text += 1;

		namelen = strnlen(state->text, state->len - (state->text - state->start));
		name = PyString_FromStringAndSize(state->text, namelen);
		if (name == NULL) {
			Py_DECREF(state->py_text);
			return NULL;
		}

		if (state->text + namelen + 20 >= state->end) {
			PyErr_SetString(PyExc_ValueError, "SHA truncated");
			Py_DECREF(name);
			Py_DECREF(state->py_text);
			return NULL;
		}

		item = Py_BuildValue("(NlN)", name, mode,
		                     sha_to_pyhex((unsigned char *)state->text+namelen+1));
		if (item == NULL) {
			Py_DECREF(name);
			Py_DECREF(state->py_text);
			return NULL;
		}

		state->text += namelen + 21;
		return item;
	} else {
		/* Raising of standard StopIteration exception with empty
		 * value. */
		PyErr_SetNone(PyExc_StopIteration);
		Py_DECREF(state->py_text);
		return NULL;
	}
}

static PyObject *py_parse_tree(PyObject *self, PyObject *args, PyObject *kw) {
	static char *kwlist[] = {"text", "strict", NULL};
	PyObject *py_text = NULL, *py_strict = NULL;
	ParseTreeIter_state *state = NULL;

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O", kwlist,
	                                 &py_text, &py_strict))
		return NULL;

	if (!PyString_Check(py_text)) {
		PyErr_SetString(PyExc_TypeError, "Text is not a string");
		return NULL;
	}

	state = PyObject_New(ParseTreeIter_state, &_objects_ParseTreeIterType);
	if (!state)
		return NULL;

	if (!PyObject_Init((PyObject*)state, &_objects_ParseTreeIterType)) {
		Py_DECREF(state);
		return NULL;
	}

	Py_INCREF(py_text);
	state->py_text = py_text;
	state->strict = py_strict ? PyObject_IsTrue(py_strict) : 0;
	state->text = PyString_AS_STRING(py_text);
	state->len = PyString_GET_SIZE(py_text);
	state->start = state->text;
	state->end = state->text + state->len;

	return (PyObject*)state;
}

struct tree_item {
	const char *name;
	int mode;
	PyObject *tuple;
};

int cmp_tree_item(const void *_a, const void *_b)
{
	const struct tree_item *a = _a, *b = _b;
	const char *remain_a, *remain_b;
	int ret, common;
	if (strlen(a->name) > strlen(b->name)) {
		common = strlen(b->name);
		remain_a = a->name + common;
		remain_b = (S_ISDIR(b->mode)?"/":"");
	} else if (strlen(b->name) > strlen(a->name)) {
		common = strlen(a->name);
		remain_a = (S_ISDIR(a->mode)?"/":"");
		remain_b = b->name + common;
	} else { /* strlen(a->name) == strlen(b->name) */
		common = 0;
		remain_a = a->name;
		remain_b = b->name;
	}
	ret = strncmp(a->name, b->name, common);
	if (ret != 0)
		return ret;
	return strcmp(remain_a, remain_b);
}

int cmp_tree_item_name_order(const void *_a, const void *_b) {
	const struct tree_item *a = _a, *b = _b;
	return strcmp(a->name, b->name);
}

static PyObject *py_sorted_tree_items(PyObject *self, PyObject *args)
{
	struct tree_item *qsort_entries = NULL;
	int name_order, num_entries, n = 0, i;
	PyObject *entries, *py_name_order, *ret, *key, *value, *py_mode, *py_sha;
	Py_ssize_t pos = 0;
	int (*cmp)(const void *, const void *);

	if (!PyArg_ParseTuple(args, "OO", &entries, &py_name_order))
		goto error;

	if (!PyDict_Check(entries)) {
		PyErr_SetString(PyExc_TypeError, "Argument not a dictionary");
		goto error;
	}

	name_order = PyObject_IsTrue(py_name_order);
	if (name_order == -1)
		goto error;
	cmp = name_order ? cmp_tree_item_name_order : cmp_tree_item;

	num_entries = PyDict_Size(entries);
	if (PyErr_Occurred())
		goto error;
	qsort_entries = PyMem_New(struct tree_item, num_entries);
	if (!qsort_entries) {
		PyErr_NoMemory();
		goto error;
	}

	while (PyDict_Next(entries, &pos, &key, &value)) {
		if (!PyString_Check(key)) {
			PyErr_SetString(PyExc_TypeError, "Name is not a string");
			goto error;
		}

		if (PyTuple_Size(value) != 2) {
			PyErr_SetString(PyExc_ValueError, "Tuple has invalid size");
			goto error;
		}

		py_mode = PyTuple_GET_ITEM(value, 0);
		if (!PyInt_Check(py_mode)) {
			PyErr_SetString(PyExc_TypeError, "Mode is not an integral type");
			goto error;
		}

		py_sha = PyTuple_GET_ITEM(value, 1);
		if (!PyString_Check(py_sha)) {
			PyErr_SetString(PyExc_TypeError, "SHA is not a string");
			goto error;
		}
		qsort_entries[n].name = PyString_AS_STRING(key);
		qsort_entries[n].mode = PyInt_AS_LONG(py_mode);

		qsort_entries[n].tuple = PyObject_CallFunctionObjArgs(
		                tree_entry_cls, key, py_mode, py_sha, NULL);
		if (qsort_entries[n].tuple == NULL)
			goto error;
		n++;
	}

	qsort(qsort_entries, num_entries, sizeof(struct tree_item), cmp);

	ret = PyList_New(num_entries);
	if (ret == NULL) {
		PyErr_NoMemory();
		goto error;
	}

	for (i = 0; i < num_entries; i++) {
		PyList_SET_ITEM(ret, i, qsort_entries[i].tuple);
	}
	PyMem_Free(qsort_entries);
	return ret;

error:
	for (i = 0; i < n; i++) {
		Py_XDECREF(qsort_entries[i].tuple);
	}
	PyMem_Free(qsort_entries);
	return NULL;
}

static PyMethodDef py_objects_methods[] = {
	{ "parse_tree", (PyCFunction)py_parse_tree, METH_VARARGS | METH_KEYWORDS,
	  NULL },
	{ "sorted_tree_items", py_sorted_tree_items, METH_VARARGS, NULL },
	{ NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC
init_objects(void)
{
	PyObject *m, *objects_mod, *errors_mod;

	m = Py_InitModule3("_objects", py_objects_methods, NULL);
	if (m == NULL)
		return;

	_objects_ParseTreeIterType.tp_new = PyType_GenericNew;
	if (PyType_Ready(&_objects_ParseTreeIterType) < 0)
		return NULL;

	errors_mod = PyImport_ImportModule("dulwich.errors");
	if (errors_mod == NULL)
		return;

	object_format_exception_cls = PyObject_GetAttrString(
		errors_mod, "ObjectFormatException");
	Py_DECREF(errors_mod);
	if (object_format_exception_cls == NULL)
		return;

	/* This is a circular import but should be safe since this module is
	 * imported at at the very bottom of objects.py. */
	objects_mod = PyImport_ImportModule("dulwich.objects");
	if (objects_mod == NULL)
		return;

	tree_entry_cls = PyObject_GetAttrString(objects_mod, "TreeEntry");
	Py_DECREF(objects_mod);
	if (tree_entry_cls == NULL)
		return;
}
