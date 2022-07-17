#include "myjql.h"
#include "buffer_pool.h"
#include "b_tree.h"
#include "table.h"
#include "str.h"
#include <stdlib.h>
typedef struct {
    RID key;
    RID value;
} Record;
void read_record(Table *table, RID rid, Record *record) {
    table_read(table, rid, (ItemPtr)record);
}
RID write_record(Table *table, const Record *record) {
    return table_insert(table, (ItemPtr)record, sizeof(Record));
}
void delete_record(Table *table, RID rid) {
    table_delete(table, rid);
}
BufferPool bufferpool0;
Table table_rec;
Table table_str;
int ptr_row_cmp(void *key, size_t key_len, RID rid) {
    Record rec;
    read_record(&table_rec, rid, &rec);
    StringRecord strrec;
    read_string(&table_str, rec.key, &strrec);
    if (get_str_chunk_size(&(strrec.chunk)) == 0) {
        return 1;
    }
    size_t charptr = 0;
    while (has_next_char(&strrec) && charptr != key_len) {
        char a = ((char*)key)[charptr++];
        char b = next_char(&table_str, &strrec);
        if (a != b) { 
            return a > b ? 1 : -1;
        }
    }
    if (charptr == key_len && has_next_char(&strrec)) {
        return -1;
    }
    if (charptr != key_len && !has_next_char(&strrec)) {
        return 1;
    }
    return 0;
}
int row_row_cmp(RID rid1, RID rid2) {
    Record rec1;
    read_record(&table_rec, rid1, &rec1);
    StringRecord strrec1;
    read_string(&table_str, rec1.key, &strrec1);
    Record rec2;
    read_record(&table_rec, rid2, &rec2);
    StringRecord strrec2;
    read_string(&table_str, rec2.key, &strrec2);
    if (get_str_chunk_size(&(strrec1.chunk)) == 0) {
        return -1;
    }
    if (get_str_chunk_size(&(strrec2.chunk)) == 0) {
        return 1;
    }
    while (has_next_char(&strrec1) && has_next_char(&strrec2)) {
        char a = next_char(&table_str, &strrec1);
        char b = next_char(&table_str, &strrec2);
        if (a != b) return a > b ? 1 : -1;
    }
    if (!has_next_char(&strrec1) && has_next_char(&strrec2)) {
        return -1;
    }
    if (has_next_char(&strrec1) && !has_next_char(&strrec2)) {
        return 1;
    }
    return 0;
}
typedef struct {
    struct InsertTemp *next;
    RID chunk;
} InsertTemp;
RID insert_handler(RID rid) {
    Record rec;
    read_record(&table_rec, rid, &rec);
    RID chunkrid = rec.key;
    InsertTemp *head = NULL;
    while (get_rid_block_addr(chunkrid) != -1) {
        InsertTemp *nd = (InsertTemp*)malloc(sizeof(InsertTemp));
        nd->chunk = chunkrid;
        nd->next = head;
        head = nd;
        StringChunk tempchunk;
        table_read(&table_str, chunkrid, &tempchunk);
        chunkrid = get_str_chunk_rid(&tempchunk);
        if (get_rid_block_addr(chunkrid) == -1) {
            break;
        }
    }
    InsertTemp *readptr = head;
    StringChunk newchunk;
    table_read(&table_str, head->chunk, &newchunk);
    short size;
    size = calc_str_chunk_size(get_str_chunk_size(&newchunk));
    RID nxtchunk;
    while (readptr != NULL) {
        nxtchunk = table_insert(&table_str, &newchunk, size);
        head = readptr->next;
        free(readptr);
        readptr = head;
        if (readptr != NULL) {
            table_read(&table_str, head->chunk, &newchunk);
            get_str_chunk_rid(&newchunk) = nxtchunk;
            size = sizeof(newchunk);
        }
    }
    Record newrec;
    newrec.key = nxtchunk;
    get_rid_block_addr(newrec.value) = -1;
    get_rid_idx(newrec.value) = -1;
    RID newrid;
    newrid = write_record(&table_rec, &newrec);
    return newrid;
}
void delete_handler(RID rid) {
    Record rec;
    read_record(&table_rec, rid, &rec);
    delete_string(&table_str, rec.key);
    delete_record(&table_rec, rid);
    return;
}
void myjql_init() {
    b_tree_init("rec.idx", &bufferpool0);
    table_init(&table_rec, "rec.data", "rec.fsm");
    table_init(&table_str, "str.data", "str.fsm");
}
void myjql_close() {

    b_tree_close(&bufferpool0);
    table_close(&table_rec);
    table_close(&table_str);
}
size_t myjql_get(const char *key, size_t key_len, char *value, size_t max_size) {
    RID kvp = b_tree_search(&bufferpool0, key, key_len, &ptr_row_cmp);
    if (get_rid_block_addr(kvp) == -1 && get_rid_idx(kvp) == 0) {
        return -1;
    }
    Record rec;
    read_record(&table_rec, kvp, &rec);
    StringRecord string_record;
    read_string(&table_str, rec.value, &string_record);
    return load_string(&table_str, &string_record, value, max_size);
}
void myjql_set(const char *key, size_t key_len, const char *value, size_t value_len) {
    RID kvp = b_tree_search(&bufferpool0, key, key_len, &ptr_row_cmp);
    if (get_rid_block_addr(kvp) == -1 && get_rid_idx(kvp) == 0) {
        RID key_rid, value_rid;
        key_rid = write_string(&table_str, key, key_len);
        value_rid = write_string(&table_str, value, value_len);
        Record rec;
        rec.key = key_rid;
        rec.value = value_rid;
        RID write_rid;
        write_rid = write_record(&table_rec, &rec);
        b_tree_insert(&bufferpool0, write_rid, &row_row_cmp, &insert_handler);
        return;
    }
    else {
        Record rec;
        read_record(&table_rec, kvp, &rec);
        delete_string(&table_str, rec.value);
        RID new_value_rid;
        new_value_rid = write_string(&table_str, value, value_len);
        b_tree_delete(&bufferpool0, kvp, &row_row_cmp, &insert_handler, &delete_handler);
        delete_record(&table_rec, kvp);
        rec.value = new_value_rid;
        RID new_kvp = write_record(&table_rec, &rec);
        b_tree_insert(&bufferpool0, new_kvp, &row_row_cmp, &insert_handler);
        return;
    }
}
void myjql_del(const char *key, size_t key_len) {
    RID kvp = b_tree_search(&bufferpool0, key, key_len, &ptr_row_cmp);
    if (get_rid_block_addr(kvp) == -1 && get_rid_idx(kvp) == 0) {
        return;
    }
    b_tree_delete(&bufferpool0, kvp, &row_row_cmp, &insert_handler, &delete_handler);
    Record rec;
    read_record(&table_rec, kvp, &rec);
    delete_string(&table_str, rec.key);
    delete_string(&table_str, rec.value);
    delete_record(&table_rec, kvp);
    return;
}
/* void myjql_analyze() {
    printf("Record Table:\n");
    analyze_table(&table_rec);
    printf("String Table:\n");
    analyze_table(&table_str);
} */