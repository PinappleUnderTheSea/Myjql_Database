#include "b_tree.h"
#include "buffer_pool.h"
#include <stdio.h>
typedef struct {
    RID child_key;
    off_t child_ptr;
} new_child;
new_child new_insert(BufferPool *pool, off_t addr, RID rid, b_tree_row_row_cmp_t cmp, off_t root_node_addr, b_tree_insert_nonleaf_handler_t insert_handler) {
    BNode *node = (BNode*)get_page(pool, addr);
    if (node->leaf) {
        if (node->n < 2 * DEGREE) {
            int ins_num = 0;
            for (; ins_num < node->n; ins_num++) {
                if((*cmp)(rid, node->row_ptr[ins_num]) < 0) {
                    break;
                }
            }
            for (int i = node->n; i > ins_num ; i--) {
                node->row_ptr[i] = node->row_ptr[i - 1];
            }
            node->row_ptr[ins_num] = rid;
            node->n++;
            release(pool, addr);
            new_child nullentry;
            get_rid_block_addr(nullentry.child_key) = -1;
            get_rid_idx(nullentry.child_key) = -1;
            nullentry.child_ptr = -1;
            return nullentry;
        }
        else {
            if (addr == root_node_addr) {
                off_t new_leaf_node_addr;
                BNode *new_leaf_node;
                BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                if (bctrl->free_node_head != -1) {
                    new_leaf_node_addr = bctrl->free_node_head;
                    new_leaf_node = (BNode*)get_page(pool, new_leaf_node_addr);
                    bctrl->free_node_head = new_leaf_node->next;
                }
                else {
                    new_leaf_node_addr = pool->file.length;
                    new_leaf_node = (BNode*)get_page(pool, pool->file.length);
                }
                off_t new_root_node_addr;
                BNode *new_root_node;
                if (bctrl->free_node_head != -1) {
                    new_root_node_addr = bctrl->free_node_head;
                    new_root_node = (BNode*)get_page(pool, new_root_node_addr);
                    bctrl->free_node_head = new_root_node->next;
                }
                else {
                    new_root_node_addr = pool->file.length;
                    new_root_node = (BNode*)get_page(pool, pool->file.length);
                }
                release(pool, 0);
                new_leaf_node->n = DEGREE + 1;
                new_leaf_node->leaf = 1;
                new_leaf_node->next = -1;
                int child_pos = 0;
                for (; child_pos != 2 * DEGREE; child_pos++) {
                    if((*cmp)(rid, node->row_ptr[child_pos]) < 0) {
                        break;
                    }
                }
                if (child_pos < DEGREE) {
                    for (int i = 0; i <= DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[DEGREE - 1 + i];
                    }
                    for (int i = DEGREE - 1; i > child_pos; i--) {
                        node->row_ptr[i] = node->row_ptr[i - 1];
                    }
                    node->row_ptr[child_pos] = rid;
                }
                else {
                    for (int i = 0; i != child_pos - DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                    }
                    new_leaf_node->row_ptr[child_pos - DEGREE] = rid;
                    for (int i = child_pos - DEGREE + 1; i <= DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[i + DEGREE - 1];
                    }
                }
                node->n = DEGREE;
                new_root_node->leaf = 0;
                new_root_node->n = 1;
                new_root_node->child[0] = addr;
                new_root_node->child[1] = new_leaf_node_addr;
                new_root_node->next = -1;
                new_root_node->row_ptr[0] = (*insert_handler)(new_leaf_node->row_ptr[0]);
                release(pool, addr);
                release(pool, new_leaf_node_addr);
                new_child newchild;
                newchild.child_key = new_root_node->row_ptr[0];
                newchild.child_ptr = new_leaf_node_addr;
                release(pool, new_root_node_addr);
                bctrl = (BCtrlBlock*)get_page(pool, 0);
                bctrl->root_node = new_root_node_addr;
                release(pool, 0);
                return newchild;
            }
            else {
                off_t new_leaf_node_addr;
                BNode *new_leaf_node;
                BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                if (bctrl->free_node_head != -1) {
                    new_leaf_node_addr = bctrl->free_node_head;
                    new_leaf_node = (BNode*)get_page(pool, new_leaf_node_addr);
                    bctrl->free_node_head = new_leaf_node->next;
                }
                else {
                    new_leaf_node_addr = pool->file.length;
                    new_leaf_node = (BNode*)get_page(pool, pool->file.length);
                }
                release(pool, 0);
                new_leaf_node->n = DEGREE + 1;
                new_leaf_node->leaf = 1;
                new_leaf_node->next = -1;
                new_child newchild;
                int child_pos = 0;
                for (; child_pos != 2 * DEGREE; child_pos++) {
                    if((*cmp)(rid, node->row_ptr[child_pos]) < 0) {
                        break;
                    }
                }
                if (child_pos < DEGREE) {
                    newchild.child_key = (*insert_handler)(node->row_ptr[DEGREE - 1]);
                    newchild.child_ptr = new_leaf_node_addr;
                    for (int i = 0; i <= DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[DEGREE - 1 + i];
                    }
                    for (int i = DEGREE - 1; i > child_pos; i--) {
                        node->row_ptr[i] = node->row_ptr[i - 1];
                    }
                    node->row_ptr[child_pos] = rid;
                }
                else {
                    for (int i = 0; i != child_pos - DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                    }
                    new_leaf_node->row_ptr[child_pos - DEGREE] = rid;
                    for (int i = child_pos - DEGREE + 1; i <= DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[i + DEGREE - 1];
                    }
                    newchild.child_key = (*insert_handler)(new_leaf_node->row_ptr[0]);
                    newchild.child_ptr = new_leaf_node_addr;
                }
                node->n = DEGREE;
                release(pool, addr);
                release(pool, new_leaf_node_addr);
                return newchild;
            }
        }
    }
    else {
        off_t child_node_addr;
        int child_node_pos = 0;
        for (; child_node_pos != node->n; child_node_pos++) {
            if ((*cmp)(rid, node->row_ptr[child_node_pos]) < 0) {
                child_node_addr = node->child[child_node_pos];
                break;
            }
        }
        child_node_addr = node->child[child_node_pos];
        new_child newchild;
        release(pool, addr);
        newchild = new_insert(pool, child_node_addr, rid, cmp, root_node_addr, insert_handler);
        node = (BNode*)get_page(pool, addr);
        if (newchild.child_ptr == -1) {
            release(pool, addr);
            return newchild;
        }
        else {
            if (node->n < 2 * DEGREE) {
                int ins_pos = 0;
                for (; ins_pos < node->n; ins_pos++) {
                    if ((*cmp)(newchild.child_key, node->row_ptr[ins_pos]) < 0) {
                        break;
                    }
                }
                for (int i = node->n; i > ins_pos; i--) {
                    node->row_ptr[i] = node->row_ptr[i - 1];
                }
                for (int i = node->n + 1; i > ins_pos + 1; i--) {
                    node->child[i] = node->child[i - 1];
                }
                node->row_ptr[ins_pos] = newchild.child_key;
                node->child[ins_pos + 1] = newchild.child_ptr;
                node->n++;
                release(pool, addr);
                new_child nullentry;
                get_rid_block_addr(nullentry.child_key) = -1;
                get_rid_idx(nullentry.child_key) = -1;
                nullentry.child_ptr = -1;
                return nullentry;
            }
            else {
                if (addr == root_node_addr) {
                    off_t new_nonleaf_node_addr;
                    BNode *new_nonleaf_node;
                    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                    if (bctrl->free_node_head != -1) {
                        new_nonleaf_node_addr = bctrl->free_node_head;
                        new_nonleaf_node = (BNode*)get_page(pool, new_nonleaf_node_addr);
                        bctrl->free_node_head = new_nonleaf_node->next;
                    }
                    else {
                        new_nonleaf_node_addr = pool->file.length;
                        new_nonleaf_node = (BNode*)get_page(pool, pool->file.length);
                    }
                    off_t new_root_node_addr;
                    BNode *new_root_node;
                    if (bctrl->free_node_head != -1) {
                        new_root_node_addr = bctrl->free_node_head;
                        new_root_node = (BNode*)get_page(pool, new_root_node_addr);
                        bctrl->free_node_head = new_root_node->next;
                    }
                    else {
                        new_root_node_addr = pool->file.length;
                        new_root_node = (BNode*)get_page(pool, pool->file.length);
                    }
                    release(pool, 0);
                    new_nonleaf_node->n = DEGREE;
                    new_nonleaf_node->leaf = 0;
                    new_nonleaf_node->next = -1;
                    new_child NCE;
                    int child_pos = 0;
                    for (; child_pos != 2 * DEGREE; child_pos++) {
                        if((*cmp)(newchild.child_key, node->row_ptr[child_pos]) < 0) {
                            break;
                        }
                    }
                    if (child_pos < DEGREE) {
                        NCE.child_key = node->row_ptr[DEGREE - 1];
                        NCE.child_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i] = node->child[DEGREE + i];
                        }
                        new_nonleaf_node->child[DEGREE] = node->child[2 * DEGREE];
                        for (int i = DEGREE - 1; i > child_pos; i--) {
                            node->row_ptr[i] = node->row_ptr[i - 1];
                            node->child[i + 1] = node->child[i];
                        }
                        node->row_ptr[child_pos] = newchild.child_key;
                        node->child[child_pos + 1] = newchild.child_ptr;
                    }
                    else if (child_pos == DEGREE) {
                        NCE.child_key = newchild.child_key;
                        NCE.child_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                        new_nonleaf_node->child[0] = newchild.child_ptr;
                    }
                    else {
                        NCE.child_key = node->row_ptr[DEGREE];
                        NCE.child_ptr = new_nonleaf_node_addr;
                        new_nonleaf_node->child[0] = node->child[DEGREE + 1];
                        for (int i = 0; i != child_pos - DEGREE - 1; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i + 1];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 2];
                        }
                        new_nonleaf_node->row_ptr[child_pos - DEGREE - 1] = newchild.child_key;
                        new_nonleaf_node->child[child_pos - DEGREE] = newchild.child_ptr;
                        for (int i = child_pos - DEGREE; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                    }
                    node->n = DEGREE;
                    release(pool, addr);
                    release(pool, new_nonleaf_node_addr);
                    new_root_node->leaf = 0;
                    new_root_node->n = 1;
                    new_root_node->child[0] = addr;
                    new_root_node->child[1] = new_nonleaf_node_addr;
                    new_root_node->next = -1;
                    new_root_node->row_ptr[0] = NCE.child_key;
                    release(pool, new_root_node_addr);
                    bctrl = (BCtrlBlock*)get_page(pool, 0);
                    bctrl->root_node = new_root_node_addr;
                    release(pool, 0);
                    return NCE;
                }
                else {
                    off_t new_nonleaf_node_addr;
                    BNode *new_nonleaf_node;
                    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                    if (bctrl->free_node_head != -1) {
                        new_nonleaf_node_addr = bctrl->free_node_head;
                        new_nonleaf_node = (BNode*)get_page(pool, new_nonleaf_node_addr);
                        bctrl->free_node_head = new_nonleaf_node->next;
                    }
                    else {
                        new_nonleaf_node_addr = pool->file.length;
                        new_nonleaf_node = (BNode*)get_page(pool, pool->file.length);
                    }
                    release(pool, 0);
                    new_nonleaf_node->n = DEGREE;
                    new_nonleaf_node->leaf = 0;
                    new_nonleaf_node->next = -1;
                    new_child NCE;
                    int child_pos = 0;
                    for (; child_pos != 2 * DEGREE; child_pos++) {
                        if((*cmp)(newchild.child_key, node->row_ptr[child_pos]) < 0) {
                            break;
                        }
                    }
                    if (child_pos < DEGREE) {
                        NCE.child_key = node->row_ptr[DEGREE - 1];
                        NCE.child_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i] = node->child[DEGREE + i];
                        }
                        new_nonleaf_node->child[DEGREE] = node->child[2 * DEGREE];
                        for (int i = DEGREE - 1; i > child_pos; i--) {
                            node->row_ptr[i] = node->row_ptr[i - 1];
                            node->child[i + 1] = node->child[i];
                        }
                        node->row_ptr[child_pos] = newchild.child_key;
                        node->child[child_pos + 1] = newchild.child_ptr;
                    }
                    else if (child_pos == DEGREE) {
                        NCE.child_key = newchild.child_key;
                        NCE.child_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                        new_nonleaf_node->child[0] = newchild.child_ptr;
                    }
                    else {
                        NCE.child_key = node->row_ptr[DEGREE];
                        NCE.child_ptr = new_nonleaf_node_addr;
                        new_nonleaf_node->child[0] = node->child[DEGREE + 1];
                        for (int i = 0; i != child_pos - DEGREE - 1; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i + 1];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 2];
                        }
                        new_nonleaf_node->row_ptr[child_pos - DEGREE - 1] = newchild.child_key;
                        new_nonleaf_node->child[child_pos - DEGREE] = newchild.child_ptr;
                        for (int i = child_pos - DEGREE; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                    }
                    node->n = DEGREE;
                    release(pool, addr);
                    release(pool, new_nonleaf_node_addr);
                    return NCE;
                }
            }
        }
    }
}
int delete(BufferPool *pool, off_t addr, off_t parent_addr, RID rid, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, b_tree_delete_nonleaf_handler_t delete_handler) {
    BNode *node = (BNode*)get_page(pool, addr);
    if (node->leaf) {
        if (parent_addr != -1) {
            if (node->n > DEGREE) {
                int idx_delete = 0;
                for(; idx_delete != node->n; idx_delete++) {
                    if((*cmp)(rid, node->row_ptr[idx_delete]) == 0) {
                        break;
                    }
                }
                for (int i = idx_delete; i != node->n - 1; i++) {
                    node->row_ptr[i] = node->row_ptr[i + 1];
                }
                node->n--;
                release(pool, addr);
                return -1;
            }
            else {
                BNode *parent_node = (BNode*)get_page(pool, parent_addr);
                int ptr_pos = 0;
                for (; ptr_pos != parent_node->n + 1; ptr_pos++) {
                    if (parent_node->child[ptr_pos] == addr) {
                        break;
                    }
                }
                if (ptr_pos != parent_node->n) {
                    off_t f_child_addr = parent_node->child[ptr_pos + 1];
                    BNode *child_page = (BNode*)get_page(pool, f_child_addr);
                    if (child_page->n > DEGREE) {
                        int idx_delete = 0;
                        for(; idx_delete != node->n; idx_delete++) {
                            if((*cmp)(rid, node->row_ptr[idx_delete]) == 0) {
                                break;
                            }
                        }
                        for (int i = idx_delete; i != node->n - 1; i++) {
                            node->row_ptr[i] = node->row_ptr[i + 1];
                        }
                        node->n--;
                        node->row_ptr[node->n] = child_page->row_ptr[0];
                        for (int i = 1; i != child_page->n; i++) {
                            child_page->row_ptr[i - 1] = child_page->row_ptr[i];
                        }
                        node->n++;
                        child_page->n--;
                        (*delete_handler)(parent_node->row_ptr[ptr_pos]);
                        parent_node->row_ptr[ptr_pos] = (*insert_handler)(child_page->row_ptr[0]);
                        release(pool, addr);
                        release(pool, parent_addr);
                        release(pool, f_child_addr);
                        return -1;
                    }
                    else {
                        release(pool, parent_addr);
                        int idx_delete = 0;
                        for(; idx_delete != node->n; idx_delete++) {
                            if((*cmp)(rid, node->row_ptr[idx_delete]) == 0) {
                                break;
                            }
                        }
                        for (int i = idx_delete; i != node->n - 1; i++) {
                            node->row_ptr[i] = node->row_ptr[i + 1];
                        }
                        node->n--;
                        for (int i = 0; i != child_page->n; i++) {
                            node->row_ptr[node->n + i] = child_page->row_ptr[i];
                        }
                        node->n += child_page->n;
                        release(pool, addr);
                        BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                        child_page->n = 0;
                        child_page->next = bctrl->free_node_head;
                        bctrl->free_node_head = f_child_addr;
                        release(pool, 0);
                        release(pool, f_child_addr);
                        return ptr_pos; 
                    }
                }
                else {
                    off_t f_child_addr = parent_node->child[ptr_pos - 1];
                    BNode *child_page = (BNode*)get_page(pool, f_child_addr);
                    if (child_page->n > DEGREE) {
                        int idx_delete = 0;
                        for(; idx_delete != node->n; idx_delete++) {
                            if((*cmp)(rid, node->row_ptr[idx_delete]) == 0) {
                                break;
                            }
                        }
                        for (int i = idx_delete; i > 0; i--) {
                            node->row_ptr[i] = node->row_ptr[i - 1];
                        }
                        node->n--;
                        node->row_ptr[0] = child_page->row_ptr[child_page->n - 1];
                        node->n++;
                        child_page->n--;
                        (*delete_handler)(parent_node->row_ptr[ptr_pos - 1]);
                        parent_node->row_ptr[ptr_pos - 1] = (*insert_handler)(node->row_ptr[0]);
                        release(pool, addr);
                        release(pool, parent_addr);
                        release(pool, f_child_addr);
                        return -1;
                    }
                    else {
                        release(pool, parent_addr);
                        int idx_delete = 0;
                        for(; idx_delete != node->n; idx_delete++) {
                            if((*cmp)(rid, node->row_ptr[idx_delete]) == 0) {
                                break;
                            }
                        }
                        for (int i = idx_delete; i != node->n - 1; i++) {
                            node->row_ptr[i] = node->row_ptr[i + 1];
                        }
                        node->n--;
                        for (int i = 0; i != node->n; i++) {
                            child_page->row_ptr[child_page->n + i] = node->row_ptr[i];
                        }
                        child_page->n += node->n;
                        release(pool, f_child_addr);
                        BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                        node->n = 0;
                        node->next = bctrl->free_node_head;
                        bctrl->free_node_head = addr;
                        release(pool, 0);
                        release(pool, addr);
                        return ptr_pos - 1; 
                    }
                }
            }
        }
        else {
            int idx_delete = 0;
            for(; idx_delete != node->n; idx_delete++) {
                if((*cmp)(rid, node->row_ptr[idx_delete]) == 0) {
                    break;
                }
            }
            for (int i = idx_delete; i != node->n - 1; i++) {
                node->row_ptr[i] = node->row_ptr[i + 1];
            }
            node->n--;
            if (node->n == 0) {
                BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                node->next = bctrl->free_node_head;
                bctrl->free_node_head = addr;
                bctrl->root_node = -1;
                release(pool, 0);
            }
            release(pool, addr);
            return -1;
        }
    }
    else {
        off_t child_node_addr;
        int child_node_pos = 0;
        for (; child_node_pos != node->n; child_node_pos++) {
            if ((*cmp)(rid, node->row_ptr[child_node_pos]) < 0) {
                child_node_addr = node->child[child_node_pos];
                break;
            }
        }
        child_node_addr = node->child[child_node_pos];
        int del_child;
        release(pool, addr);
        del_child = delete(pool, child_node_addr, addr, rid, cmp, insert_handler, delete_handler);
        node = (BNode*)get_page(pool, addr);
        if (del_child == -1) {
            release(pool, addr);
            return -1;
        }
        else {
            if (parent_addr != -1) {
                if (node->n > DEGREE) {
                    BNode *childnode = (BNode*)get_page(pool, node->child[0]);
                    if (childnode->leaf) {
                        (*delete_handler)(node->row_ptr[del_child]); 
                    }
                    release(pool, node->child[0]);
                    for (int i = del_child; i != node->n - 1; i++) {
                        node->row_ptr[i] = node->row_ptr[i + 1];
                        node->child[i + 1] = node->child[i + 2];
                    }
                    node->n--;
                    release(pool, addr);
                    return -1;
                }
                else {
                    BNode *parent_node = (BNode*)get_page(pool, parent_addr);
                    int ptr_pos = 0;
                    for (; ptr_pos != parent_node->n + 1; ptr_pos++) {
                        if (parent_node->child[ptr_pos] == addr) {
                            break;
                        }
                    }
                    if (ptr_pos != parent_node->n) {
                        off_t f_child_addr = parent_node->child[ptr_pos + 1];
                        BNode *child_page = (BNode*)get_page(pool, f_child_addr);
                        if (child_page->n > DEGREE) {
                            BNode *childnode = (BNode*)get_page(pool, node->child[0]);
                            if (childnode->leaf) {
                                (*delete_handler)(node->row_ptr[del_child]); 
                            }
                            release(pool, node->child[0]);
                            for (int i = del_child; i != node->n - 1; i++) {
                                node->row_ptr[i] = node->row_ptr[i + 1];
                                node->child[i + 1] = node->child[i + 2];
                            }
                            node->n--;
                            node->row_ptr[node->n] = parent_node->row_ptr[ptr_pos];
                            node->child[node->n + 1] = child_page->child[0];
                            node->n++;
                            parent_node->row_ptr[ptr_pos] = child_page->row_ptr[0];
                            for (int i = 1; i != child_page->n; i++) {
                                child_page->row_ptr[i - 1] = child_page->row_ptr[i];
                                child_page->child[i - 1] = child_page->child[i];
                            }
                            child_page->child[child_page->n - 1] = child_page->child[child_page->n];
                            child_page->n--;
                            release(pool, addr);
                            release(pool, parent_addr);
                            release(pool, f_child_addr);
                            return -1;
                        }
                        else {
                            BNode *childnode = (BNode*)get_page(pool, node->child[0]);
                            if (childnode->leaf) {
                                (*delete_handler)(node->row_ptr[del_child]); 
                            }
                            release(pool, node->child[0]);
                            for (int i = del_child; i != node->n - 1; i++) {
                                node->row_ptr[i] = node->row_ptr[i + 1];
                                node->child[i + 1] = node->child[i + 2];
                            }
                            node->n--;
                            node->row_ptr[node->n] = parent_node->row_ptr[ptr_pos];
                            node->child[node->n + 1] = child_page->child[0];
                            node->n++;
                            release(pool, parent_addr);
                            for (int i = 0; i != child_page->n; i++) {
                                node->row_ptr[node->n + i] = child_page->row_ptr[i];
                                node->child[node->n + i + 1] = child_page->child[i + 1];
                            }
                            node->n += child_page->n;
                            release(pool, addr);
                            BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                            child_page->n = 0;
                            child_page->next = bctrl->free_node_head;
                            bctrl->free_node_head = f_child_addr;
                            release(pool, 0);
                            release(pool, f_child_addr);
                            return ptr_pos; 
                        }
                    }
                    else {
                        off_t f_child_addr = parent_node->child[ptr_pos - 1];
                        BNode *child_page = (BNode*)get_page(pool, f_child_addr);
                        if (child_page->n > DEGREE) {
                            BNode *childnode = (BNode*)get_page(pool, node->child[0]);
                            if (childnode->leaf) {
                                (*delete_handler)(node->row_ptr[del_child]); 
                            }
                            release(pool, node->child[0]);
                            for (int i = del_child; i != node->n - 1; i++) {
                                node->row_ptr[i] = node->row_ptr[i + 1];
                                node->child[i + 1] = node->child[i + 2];
                            }
                            node->n--;
                            for (int i = node->n; i > 0; i--) {
                                node->row_ptr[i] = node->row_ptr[i - 1];
                                node->child[i + 1] = node->child[i];
                            }
                            node->child[1] = node->child[0];
                            node->n++;
                            node->row_ptr[0] = parent_node->row_ptr[ptr_pos - 1];
                            node->child[0] = child_page->child[child_page->n];
                            parent_node->row_ptr[ptr_pos - 1] = child_page->row_ptr[child_page->n - 1];
                            child_page->n--;
                            release(pool, addr);
                            release(pool, parent_addr);
                            release(pool, f_child_addr);
                            return -1;
                        }
                        else {
                            BNode *childnode = (BNode*)get_page(pool, node->child[0]);
                            if (childnode->leaf) {
                                (*delete_handler)(node->row_ptr[del_child]); 
                            }
                            release(pool, node->child[0]);
                            for (int i = del_child; i != node->n - 1; i++) {
                                node->row_ptr[i] = node->row_ptr[i + 1];
                                node->child[i + 1] = node->child[i + 2];
                            }
                            node->n--;
                            child_page->row_ptr[child_page->n] = parent_node->row_ptr[ptr_pos - 1];
                            child_page->child[child_page->n + 1] = node->child[0];
                            release(pool, parent_addr);
                            child_page->n++;
                            for (int i = 0; i != node->n; i++) {
                                child_page->row_ptr[child_page->n + i] = node->row_ptr[i];
                                child_page->child[child_page->n + i + 1] = node->child[i + 1];
                            }
                            child_page->n += node->n;
                            release(pool, f_child_addr);
                            BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                            node->n = 0;
                            node->next = bctrl->free_node_head;
                            bctrl->free_node_head = addr;
                            release(pool, 0);
                            release(pool, addr);
                            return ptr_pos - 1;
                        }
                    }
                }
            }
            else {
                BNode *childnode = (BNode*)get_page(pool, node->child[0]);
                if (childnode->leaf) {
                    (*delete_handler)(node->row_ptr[del_child]); 
                }
                release(pool, node->child[0]);
                for (int i = del_child; i != node->n - 1; i++) {
                    node->row_ptr[i] = node->row_ptr[i + 1];
                    node->child[i + 1] = node->child[i + 2];
                }
                node->n--;
                if (node->n == 0) {
                    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                    node->next = bctrl->free_node_head;
                    bctrl->free_node_head = addr;
                    bctrl->root_node = node->child[0];
                    release(pool, 0);
                }
                release(pool, addr);
                return -1;
            }
        }
    }
}
void b_tree_init(const char *filename, BufferPool *pool) {
    init_buffer_pool(filename, pool);
    if (pool->file.length != 0) {
        return;
    }
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    bctrl->free_node_head = -1;
    bctrl->root_node = -1;
    release(pool, 0);
}
void b_tree_close(BufferPool *pool) {
    close_buffer_pool(pool);
}
RID b_tree_search(BufferPool *pool, void *key, size_t size, b_tree_ptr_row_cmp_t cmp) {
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    off_t root_node_addr = bctrl->root_node;
    release(pool, 0);
    if (root_node_addr == -1) {
        RID ans;
        get_rid_block_addr(ans) = -1;
        get_rid_idx(ans) = 0;
        return ans;
    }
    BNode *node = (BNode*)get_page(pool, root_node_addr);
    off_t addr = root_node_addr;
    while (!node->leaf) {
        int child_pos = 0;
        for (; child_pos != node->n; child_pos++) {
            if ((*cmp)(key, size, node->row_ptr[child_pos]) < 0) {
                break;
            }
        }
        off_t child_addr = node->child[child_pos];
        release(pool, addr);
        addr = child_addr;
        node = (BNode*)get_page(pool, addr);
    }
    for (int i = 0; i != node->n; i++) {
        if ((*cmp)(key, size, node->row_ptr[i]) == 0) {
            RID ans = node->row_ptr[i];
            release(pool, addr);
            return ans;
        }
    }
    release(pool, addr);
    RID ans;
    get_rid_block_addr(ans) = -1;
    get_rid_idx(ans) = 0;
    return ans;
}
RID b_tree_insert(BufferPool *pool, RID rid, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler) {
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    off_t root_node_addr = bctrl->root_node;
    BNode *root;
    if (root_node_addr == -1) {
        if (bctrl->free_node_head != -1) {
            root = (BNode*)get_page(pool, bctrl->free_node_head);
            bctrl->free_node_head = root->next;
        }
        else {
            root = (BNode*)get_page(pool, PAGE_SIZE);
        }
        root->n = 0;
        root->next = -1;
        root->leaf = 1;
        bctrl->root_node = PAGE_SIZE;
    }
    else {
        root = (BNode*)get_page(pool, root_node_addr);
    }
    root_node_addr = bctrl->root_node;
    release(pool, 0);
    release(pool, root_node_addr);
    new_insert(pool, root_node_addr, rid, cmp, root_node_addr, insert_handler);
    return rid;
}
void b_tree_delete(BufferPool *pool, RID rid, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, b_tree_delete_nonleaf_handler_t delete_handler) {
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    off_t root_node_addr = bctrl->root_node;
    if (root_node_addr == -1) {
        return;
    }
    release(pool, 0);
    delete(pool, root_node_addr, -1, rid, cmp, insert_handler, delete_handler);
}