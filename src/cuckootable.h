// Include guard
#ifndef INT_STR_TABLE_H
#define INT_STR_TABLE_H

// Below we define the constants the table template uses to fill in the
// interface.
//
// All table functions will be prefixed with `int_str_table`
#define CUCKOO_TABLE_NAME cuckoo_table
// The type of the key is `const char *`
#define CUCKOO_KEY_TYPE void *
// The type of the mapped value is `const char *`
#define CUCKOO_MAPPED_TYPE void *

// Including the header after filling in the constants will populate the
// interface. See the template file itself for specific function names; most of
// them correspond to methods in the C++ implementation.
#include "libcuckoo-c/cuckoo_table_template.h"

#endif // INT_STR_TABLE_H
