/* 
 * Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2
 * of the License or (at your option) a later version.
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
#include <stdint.h>


static size_t get_delta_header_size(uint8_t *delta, int *index, int length)
{
	size_t size = 0;
	int i = 0;
	while ((*index) < length) {
		uint8_t cmd = delta[*index];
		(*index)++;
		size |= (cmd & ~0x80) << i;
		i += 7;
		if (!(cmd & 0x80))
			break;
	}
	return size;
}


static PyObject *py_apply_delta(PyObject *self, PyObject *args)
{
	uint8_t *src_buf, *delta;
	int src_buf_len, delta_len;
	size_t src_size, dest_size;
	size_t outindex = 0;
	int index;
	uint8_t *out;
	PyObject *ret;

	if (!PyArg_ParseTuple(args, "s#s#", (uint8_t *)&src_buf, &src_buf_len, 
						  (uint8_t *)&delta, &delta_len))
		return NULL;

    index = 0;
    src_size = get_delta_header_size(delta, &index, delta_len);
    if (src_size != src_buf_len) {
		PyErr_Format(PyExc_ValueError, 
			"Unexpected source buffer size: %lu vs %d", src_size, src_buf_len);
		return NULL;
	}
    dest_size = get_delta_header_size(delta, &index, delta_len);
	ret = PyString_FromStringAndSize(NULL, dest_size);
	if (ret == NULL) {
		PyErr_NoMemory();
		return NULL;
	}
	out = (uint8_t *)PyString_AsString(ret);
    while (index < delta_len) {
        char cmd = delta[index];
        index++;
        if (cmd & 0x80) {
            size_t cp_off = 0, cp_size = 0;
			int i;
            for (i = 0; i < 4; i++) {
                if (cmd & (1 << i)) {
                    uint8_t x = delta[index];
                    index++;
                    cp_off |= x << (i * 8);
				}
			}
            for (i = 0; i < 3; i++) {
                if (cmd & (1 << (4+i))) {
                    uint8_t x = delta[index];
                    index++;
                    cp_size |= x << (i * 8);
				}
			}
            if (cp_size == 0)
                cp_size = 0x10000;
            if (cp_off + cp_size < cp_size ||
                cp_off + cp_size > src_size ||
                cp_size > dest_size)
                break;
			memcpy(out+outindex, src_buf+cp_off, cp_size);
			outindex += cp_size;
		} else if (cmd != 0) {
			memcpy(out+outindex, delta+index, cmd);
			outindex += cmd;
            index += cmd;
		} else {
			PyErr_SetString(PyExc_ValueError, "Invalid opcode 0");
			return NULL;
		}
	}
    
    if (index != delta_len) {
		PyErr_SetString(PyExc_ValueError, "delta not empty");
		return NULL;
	}

	if (dest_size != outindex) {
        PyErr_SetString(PyExc_ValueError, "dest size incorrect");
		return NULL;
	}

    return ret;
}

static PyMethodDef py_pack_methods[] = {
	{ "apply_delta", (PyCFunction)py_apply_delta, METH_VARARGS, NULL },
};

void init_pack(void)
{
	PyObject *m;

	m = Py_InitModule3("_pack", py_pack_methods, NULL);
	if (m == NULL)
		return;
}
