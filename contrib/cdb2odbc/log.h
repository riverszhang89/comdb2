/**
 * @file log.h
 * @description
 * @author Rivers Zhang <hzhang320@bloomberg.net>
 * @history
 * 08-Aug-2014 Created.
 */

#ifndef _LOG_
#define _LOG_

/* ANSI escape code for colors. */
#ifdef __COLORFUL__
# define NRM  "\x1B[0m"
# define RED  "\x1B[31m"
# define GRN  "\x1B[32m"
# define YEL  "\x1B[33m"
# define MAG  "\x1B[36m"
# define WHT  "\x1B[37m"
#else
# define NRM
# define RED
# define GRN
# define YEL
# define MAG
# define WHT
#endif /* __COLORFUL__ */

#ifndef __LOG__
# define __log(lvl, flag, ...) do {} while(0)
#else

# define LDEBUG NRM
# define LINFO GRN
# define LWARN YEL
# define LFATAL RED

# define LOG_D 0x1
# define LOG_I 0x2
# define LOG_W 0x4
# define LOG_F 0x8

# if defined(__DEBUG__)
#  define LOG_LVL LOG_D
# elif defined(__INFO__)
#  define LOG_LVL LOG_I
# elif defined(__FATAL__)
#  define LOG_LVL LOG_F
# else
#  define LOG_LVL LOG_W
# endif
 
# define __log(lvl, flag, ...) do {   \
     if(lvl >= LOG_LVL) {   \
         printf("[COMDB2-ODBC][%s" NRM "][" \
                 MAG "%s" NRM"][" MAG "%s" NRM "] at line " MAG "%d" NRM ": ", flag, __FILE__ , __func__, __LINE__); \
         printf(__VA_ARGS__);   \
         printf("\n");  \
     }  \
} while(0)
#endif /* __LOG__ */

# define __debug(args...) __log(LOG_D, LDEBUG "DEBUG", ## args)
# define __info(args...) __log(LOG_I, LINFO "INFO", ## args)
# define __warn(args...) __log(LOG_W, LWARN "WARN", ## args)
# define __fatal(args...) __log(LOG_F, LFATAL "FATAL", ## args)

#endif
