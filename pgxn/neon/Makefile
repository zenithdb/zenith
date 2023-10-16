# pgxs/neon/Makefile


MODULE_big = neon
OBJS = \
	$(WIN32RES) \
	extension_server.o \
	file_cache.o \
	libpagestore.o \
	neon.o \
	neon_utils.o \
	pagestore_smgr.o \
	relsize_cache.o \
	walproposer.o \
	walproposer_pg.o \
	control_plane_connector.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)
SHLIB_LINK = -lcurl

EXTENSION = neon
DATA = neon--1.0.sql neon--1.0--1.1.sql
PGFILEDESC = "neon - cloud storage for PostgreSQL"


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
