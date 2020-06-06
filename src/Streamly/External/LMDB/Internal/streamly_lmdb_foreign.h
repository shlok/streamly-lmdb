#include <lmdb.h>

int mdb_put_(MDB_txn *txn, MDB_dbi dbi, char *key, size_t key_size, char *val, size_t val_size, unsigned int flags);
