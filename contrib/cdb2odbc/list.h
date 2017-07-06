/**
 * @file list.h
 * @description Doubly linked list. 
 * @author Rivers Zhang <hzhang320@bloomberg.net>

 * @history
 * 16-Jun-2014 Created.
 * 18-Jun-2014 More list manipulation methods added:
 *              prepend/append a node; romove a node.
 */

#ifndef _LIST_H_
#define _LIST_H_

/**
 * A node in a doubly linked list.
 * @next and @prev point to the next and previous node in the doubly linked list respectively.
 */
typedef struct list_head {
    struct list_head *next, *prev;
} list_head_t;

typedef list_head_t list_t;

/**
 * Initialize a list.
 */
static inline void INIT_LIST_HEAD(list_t *list)
{
    list->next = list;
    list->prev = list;
}

static inline void __list_add(list_head_t *_new,
                              list_head_t *prev,
                              list_head_t *next)
{
    next->prev = _new;
    _new->next = next;
    _new->prev = prev;
    prev->next = _new;
}

/**
 * Append @_new to @head.
 */
static inline void list_append(list_head_t *_new, list_head_t *head)
{
    __list_add(_new, head, head->next);
}

/**
 * Prepend @_new to @head.
 */
static inline void list_prepend(list_head_t *_new, list_head_t *head)
{
    __list_add(_new, head->prev, head);
}

/**
 * Is the list empty?
 */
static inline int list_empty(const list_t *head)
{
    return head->next == head;
}

static inline void __list_rm(list_head_t *prev, list_head_t *next)
{
    next->prev = prev;
    prev->next = next;
}

#define LIST_POISON1  ((void *) 0x00100100)
#define LIST_POISON2  ((void *) 0x00200200)

/**
 * Remove @entry from the list.
 */
static inline void list_rm(list_head_t *entry)
{
    __list_rm(entry->prev, entry->next);
    entry->next = (list_head_t *)LIST_POISON1;
    entry->prev = (list_head_t *)LIST_POISON2;
}

#define LIST_HEAD_INIT(name) {&(name), &(name)}

/**
 * Macro version of INIT_LIST_HEAD.
 */
#define LIST_HEAD(name) \
    list_head_t name = LIST_HEAD_INIT(name)

/* Undefine @offsetof. Sparc has defined it. */
#undef offsetof
#define offsetof(type, member) ( (size_t) &( (type *)0 )->member )

#ifdef _AIX
/* XLC has __typeof__. It does exactly the same thing as typeof in GCC. */
#define typeof __typeof__
#endif

/**
 * Cast the variable who contains @ptr to @type.
 * @ptr    - a list_head_t variable
 * @type   - to what @type
 * @member - the name of list_head_t variable defined in @type
 */
#define cast_to(ptr, type, member) ({                               \
        const typeof( ( (type *)0 )->member ) *__mptr = (ptr);      \
        (type *)( (char *)__mptr - offsetof(type, member) );})

/**
 * Return the variable who contains @ptr as a @type variable.
 * @ptr    - a list_head_t variable
 * @type   - to what @type
 * @member - the name of list_head_t variable defined in @type
 */
#define list_entry(ptr, type, member)   \
        cast_to(ptr, type, member)

/**
 * Return the variable who contains @ptr->next as a @type variable.
 * @ptr    - a list_head_t variable
 * @type   - to what @type
 * @member - the name of list_head_t variable defined in @type
 */
#define list_first_entry(ptr, type, member) \
        list_entry( (ptr)->next, type, member ) 

/**
 * Return the next entry of @pos.
 * @pos    - where I am in the doubly linked list. 
 * @member - the name of list_head_t variable defined in the type of @pos 
 */
#define list_next_entry(pos, member)    \
        list_entry( (pos)->member.next, typeof( *(pos) ), member )

/**
 * Iterate over a list.
 * @pos    - @pos will be pointed to the address of next node. 
             So it should be a pointer. 
 * @head    - where I am in the doubly linked list. 
 * @member - the name of list_head_t variable defined in the type of @pos 
 */
#define list_iterate(pos, head, member)                                     \
        for(pos = list_first_entry(head, typeof( *(pos) ), member);         \
            &pos->member != (head);                                         \
            pos = list_next_entry(pos, member))

/* Iterate over a list of given type safe against removal of list entry. */
#define list_iterate_safe(pos, n, head, member)                             \
        for (pos = list_first_entry(head, typeof( *(pos) ), member),        \
            n = list_next_entry(pos, member);                               \
            &pos->member != (head);                                         \
            pos = n, n = list_next_entry(n, member))

#endif /* _LIST_H_ */
