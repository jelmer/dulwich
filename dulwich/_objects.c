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

#define bytehex(x) (((x)<0xa)?('0'+(x)):('a'-0xa+(x)))

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

static PyObject *py_parse_tree(PyObject *self, PyObject *args)
{
	char *text, *end;
	int len, namelen;
	PyObject *ret, *item, *name;

	if (!PyArg_ParseTuple(args, "s#", &text, &len))
		return NULL;

	ret = PyDict_New();
	if (ret == NULL) {
		return NULL;
	}

	end = text + len;

    while (text < end) {
        long mode;
		mode = strtol(text, &text, 8);

		if (*text != ' ') {
			PyErr_SetString(PyExc_RuntimeError, "Expected space");
			Py_DECREF(ret);
			return NULL;
		}

		text++;

        namelen = strlen(text);

		name = PyString_FromStringAndSize(text, namelen);
		if (name == NULL) {
			Py_DECREF(ret);
			return NULL;
		}

        item = Py_BuildValue("(lN)", mode, sha_to_pyhex((unsigned char *)text+namelen+1));
        if (item == NULL) {
            Py_DECREF(ret);
			Py_DECREF(name);
            return NULL;
        }
		if (PyDict_SetItem(ret, name, item) == -1) {
			Py_DECREF(ret);
			Py_DECREF(item);
			return NULL;
		}
		Py_DECREF(name);
		Py_DECREF(item);

		text += namelen+21;
    }

    return ret;
}

static PyMethodDef py_objects_methods[] = {
	{ "parse_tree", (PyCFunction)py_parse_tree, METH_VARARGS, NULL },
	{ NULL, NULL, 0, NULL }
};

void init_objects(void)
{
	PyObject *m;

	m = Py_InitModule3("_objects", py_objects_methods, NULL);
	if (m == NULL)
		return;
}
