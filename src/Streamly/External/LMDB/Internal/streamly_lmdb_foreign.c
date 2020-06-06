#include "streamly_lmdb_foreign.h"

int mdb_put_(MDB_txn *txn, MDB_dbi dbi, char *key, size_t key_size, char *val, size_t val_size, unsigned int flags) {
    MDB_val k;
    k.mv_data = key;
    k.mv_size = key_size;
    MDB_val v;
    v.mv_data = val;
    v.mv_size = val_size;
    return mdb_put(txn, dbi, &k, &v, flags);
}
