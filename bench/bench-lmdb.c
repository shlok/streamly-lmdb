#include <dirent.h>
#include <lmdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define E(func, ...)                                                           \
  do {                                                                         \
    if (func(__VA_ARGS__) != 0) {                                              \
      printf("%s error (%s:%d)\n", #func, __FILE__, __LINE__);                 \
      return 1;                                                                \
    }                                                                          \
  } while (0)

void print_usage();
void __attribute__((noinline))
copy_bytes(char *memory, int factor, int x, int offset);

int main(int argc, char *argv[]) {
  if (argc <= 2) {
    print_usage();
    return 1;
  }
  const char *path = argv[2];
  MDB_env *env;
  unsigned long long kb = 1024ULL;
  unsigned long long tb = kb * kb * kb * kb;
  E(mdb_env_create, &env);
  E(mdb_env_set_mapsize, env, tb);
  if (strcmp(argv[1], "write") == 0) {
    if (argc != 7) {
      print_usage();
      return 1;
    }
    int key_factor = strtol(argv[3], NULL, 10);
    int value_factor = strtol(argv[4], NULL, 10);
    long long num_pairs = strtoll(argv[5], NULL, 10);
    int chunk_size = strtol(argv[6], NULL, 10);
    int valid_write_args = key_factor > 0 && value_factor > 0 &&
                           0 < num_pairs && chunk_size > 0 &&
                           chunk_size <= num_pairs;
    if (!valid_write_args) {
      printf("Invalid write arguments.\n");
      print_usage();
      return 1;
    }
    struct stat buffer;
    if (stat(path, &buffer) == 0) {
      printf("File already exists at %s\n", path);
      return 1;
    }
    E(mkdir, path, 0770);

    MDB_txn *txn;
    MDB_dbi dbi;
    E(mdb_env_open, env, path, 0, 0664);
    E(mdb_txn_begin, env, NULL, MDB_RDONLY, &txn);
    E(mdb_dbi_open, txn, NULL, 0, &dbi);
    E(mdb_txn_commit, txn);

    char *key_data = calloc(8 * key_factor, 1);
    char *val_data = calloc(8 * value_factor, 1);

    int current_chunk_size = 0;
    long long i = 0;
    for (int x0 = 0; x0 <= 255; x0++) {
      copy_bytes(key_data, key_factor, x0, 4);
      copy_bytes(val_data, value_factor, x0, 4);
      for (int x1 = 0; x1 <= 255; x1++) {
        copy_bytes(key_data, key_factor, x1, 5);
        copy_bytes(val_data, value_factor, x1, 5);
        for (int x2 = 0; x2 <= 255; x2++) {
          copy_bytes(key_data, key_factor, x2, 6);
          copy_bytes(val_data, value_factor, x2, 6);
          for (int x3 = 0; x3 <= 255; x3++) {
            copy_bytes(key_data, key_factor, x3, 7);
            copy_bytes(val_data, value_factor, x3, 7);
            if (i >= num_pairs) {
              E(mdb_txn_commit, txn);
              return 0; // Complete.
            }
            if (current_chunk_size >= chunk_size) {
              E(mdb_txn_commit, txn);
              current_chunk_size = 0;
            }
            if (current_chunk_size == 0) {
              E(mdb_txn_begin, env, NULL, 0, &txn);
            }

            MDB_val key;
            key.mv_size = 8 * key_factor;
            key.mv_data = key_data;

            MDB_val value;
            value.mv_size = 8 * value_factor;
            value.mv_data = val_data;

            E(mdb_put, txn, dbi, &key, &value, 0);

            current_chunk_size++;
            i++;
          }
        }
      }
    }
    printf("Overflow.\n");
    return 1;
  } else if (strcmp(argv[1], "read-cursor") == 0) {
    if (argc != 3) {
      print_usage();
      return 1;
    }
    E(mdb_env_open, env, path, 0, 0664);
    MDB_txn *txn;
    MDB_dbi dbi;
    E(mdb_txn_begin, env, NULL, MDB_RDONLY, &txn);
    E(mdb_dbi_open, txn, NULL, 0, &dbi);
    E(mdb_txn_commit, txn);

    E(mdb_txn_begin, env, NULL, MDB_RDONLY, &txn);
    MDB_cursor *curs;
    E(mdb_cursor_open, txn, dbi, &curs);

    long long key_byte_count = 0;
    long long value_byte_count = 0;
    long long total_byte_count = 0;
    long long pair_count = 0;
    MDB_cursor_op op = MDB_FIRST;
    MDB_val key;
    MDB_val val;
    int rc;
    do {
      rc = mdb_cursor_get(curs, &key, &val, op);
      if (rc != 0 && rc != MDB_NOTFOUND) {
        printf("cursor_get error\n");
        return 1;
      }
      if (op == MDB_FIRST) {
        op = MDB_NEXT;
      }
      if (rc != MDB_NOTFOUND) {
        key_byte_count += key.mv_size;
        value_byte_count += val.mv_size;
        total_byte_count += key.mv_size + val.mv_size;
        pair_count++;
      }
    } while (rc != MDB_NOTFOUND);
    mdb_cursor_close(curs);
    E(mdb_txn_commit, txn);
    printf("Key byte count:   %lld\n", key_byte_count);
    printf("Value byte count: %lld\n", value_byte_count);
    printf("Total byte count: %lld\n", total_byte_count);
    printf("Pair count:       %lld\n", pair_count);
  } else if (strcmp(argv[1], "read-keys") == 0) {
    if (argc != 4) {
      print_usage();
      return 1;
    }
    int key_factor = strtol(argv[3], NULL, 10);

    E(mdb_env_open, env, path, 0, 0664);
    MDB_txn *txn;
    MDB_dbi dbi;
    E(mdb_txn_begin, env, NULL, MDB_RDONLY, &txn);
    E(mdb_dbi_open, txn, NULL, 0, &dbi);
    E(mdb_txn_commit, txn);

    E(mdb_txn_begin, env, NULL, MDB_RDONLY, &txn);

    char *key_data = calloc(8 * key_factor, 1);

    long long key_byte_count = 0;
    long long value_byte_count = 0;
    long long total_byte_count = 0;
    long long pair_count = 0;
    for (int x0 = 0; x0 <= 255; x0++) {
      copy_bytes(key_data, key_factor, x0, 4);
      for (int x1 = 0; x1 <= 255; x1++) {
        copy_bytes(key_data, key_factor, x1, 5);
        for (int x2 = 0; x2 <= 255; x2++) {
          copy_bytes(key_data, key_factor, x2, 6);
          for (int x3 = 0; x3 <= 255; x3++) {
            copy_bytes(key_data, key_factor, x3, 7);
            MDB_val key;
            key.mv_size = 8 * key_factor;
            key.mv_data = key_data;

            MDB_val value;
            value.mv_size = -1;
            value.mv_data = NULL;
            // Profiling shows that most of the time (by orders of magnitude) is
            // spent in mdb_get().
            int rc = mdb_get(txn, dbi, &key, &value);
            if (rc != 0 && rc != MDB_NOTFOUND) {
              printf("mdb_get error\n");
              return 1;
            } else if (rc == MDB_NOTFOUND) {
              E(mdb_txn_commit, txn);
              printf("Key byte count:   %lld\n", key_byte_count);
              printf("Value byte count: %lld\n", value_byte_count);
              printf("Total byte count: %lld\n", total_byte_count);
              printf("Pair count:       %lld\n", pair_count);
              return 0;
            } else {
              key_byte_count += key.mv_size;
              value_byte_count += value.mv_size;
              total_byte_count += key.mv_size + value.mv_size;
              pair_count++;
            }
          }
        }
      }
    }
    printf("Overflow.\n");
    return 1;
  } else if (strcmp(argv[1], "read-files") == 0) {
    // Read all files in path (non-recursively), read one byte from each file
    // (as an int), and output the sum of the integers.
    DIR *dir;
    if ((dir = opendir(path)) != NULL) {
      FILE *fp;
      struct dirent *ent;
      long long sum = 0;
      long long file_count = 0;
      while ((ent = readdir(dir)) != NULL) {
        if (ent->d_type == DT_REG) { // Read only regular files.
          char f_path[256];
          if (snprintf(f_path, 256, "%s/%s", path, ent->d_name) >= 256) {
            printf("Error: unexpected truncation.\n");
            return 1;
          };
          fp = fopen(f_path, "r");
          int byte = fgetc(fp);
          sum += (long)byte;
          file_count++;
          fclose(fp);
        };
      }
      printf("File count: %lld\n", file_count);
      printf("Sum: %lld\n", sum);
      closedir(dir);
    } else {
      printf("Error: Unable to open directory: %s\n", path);
      return 1;
    }
  } else {
    print_usage();
    return 1;
  }

  return 0;
}

void print_usage() {
  printf("bench-lmdb write [path] [key factor] [value factor] \
            [# key-value pairs] [# pairs in each transaction]\n");
  printf("bench-lmdb read-cursor [path]\n");
  printf("bench-lmdb read-keys [path] [key factor]\n");
  printf("bench-lmdb read-files [path]\n");
}

void copy_bytes(char *memory, int factor, int x, int offset) {
  for (int i = 0; i < factor; i++) {
    *(memory + 8 * i + offset) = (unsigned char)x;
  }
}
