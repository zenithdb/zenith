/*-------------------------------------------------------------------------
 *
 * control_plane_connector.c
 *	  Captures updates to roles/databases using ProcessUtility_hook and
 *        sends them to the control ProcessUtility_hook. The changes are sent
 *        via HTTP to the URL specified by the GUC neon.console_url when the
 *        transaction commits. Forwarding may be disabled temporarily by
 *        setting neon.forward_ddl to false.
 *
 *        Currently, the transaction may abort AFTER
 *        changes have already been forwarded, and that case is not handled.
 *        Subtransactions are handled using a stack of hash tables, which
 *        accumulate changes. On subtransaction commit, the top of the stack
 *        is merged with the table below it.
 *
 * IDENTIFICATION
 *	 contrib/neon/control_plane_connector.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <curl/curl.h>

#include "access/xact.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/jsonb.h"

#include "control_plane_connector.h"
#include "neon_utils.h"

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

static const char *jwt_token = NULL;

/* GUCs */
static char *ConsoleURL = NULL;
static bool ForwardDDL = true;
static bool RegressTestMode = false;

/*
 * CURL docs say that this buffer must exist until we call curl_easy_cleanup
 * (which we never do), so we make this a static
 */
static char CurlErrorBuf[CURL_ERROR_SIZE];

typedef enum
{
	Op_Set,						/* An upsert: Either a creation or an alter */
	Op_Delete,
} OpType;

typedef struct
{
	char		name[NAMEDATALEN];
	Oid			owner;
	char		old_name[NAMEDATALEN];
	OpType		type;
} DbEntry;

typedef struct
{
	char		name[NAMEDATALEN];
	char		old_name[NAMEDATALEN];
	const char *password;
	OpType		type;
} RoleEntry;

/*
 * We keep one of these for each subtransaction in a stack. When a subtransaction
 * commits, we merge the top of the stack into the table below it. It is allocated in the
 * subtransaction's context.
 */
typedef struct DdlHashTable
{
	struct DdlHashTable *prev_table;
	HTAB	   *db_table;
	HTAB	   *role_table;
} DdlHashTable;

static DdlHashTable RootTable;
static DdlHashTable *CurrentDdlTable = &RootTable;

static void
PushKeyValue(JsonbParseState **state, char *key, char *value)
{
	JsonbValue	k,
				v;

	k.type = jbvString;
	k.val.string.len = strlen(key);
	k.val.string.val = key;
	v.type = jbvString;
	v.val.string.len = strlen(value);
	v.val.string.val = value;
	pushJsonbValue(state, WJB_KEY, &k);
	pushJsonbValue(state, WJB_VALUE, &v);
}

static char *
ConstructDeltaMessage()
{
	JsonbParseState *state = NULL;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	if (RootTable.db_table)
	{
		JsonbValue	dbs;
		HASH_SEQ_STATUS status;
		DbEntry    *entry;

		dbs.type = jbvString;
		dbs.val.string.val = "dbs";
		dbs.val.string.len = strlen(dbs.val.string.val);
		pushJsonbValue(&state, WJB_KEY, &dbs);
		pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

		hash_seq_init(&status, RootTable.db_table);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
			PushKeyValue(&state, "op", entry->type == Op_Set ? "set" : "del");
			PushKeyValue(&state, "name", entry->name);
			if (entry->owner != InvalidOid)
			{
				PushKeyValue(&state, "owner", GetUserNameFromId(entry->owner, false));
			}
			if (entry->old_name[0] != '\0')
			{
				PushKeyValue(&state, "old_name", entry->old_name);
			}
			pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		}
		pushJsonbValue(&state, WJB_END_ARRAY, NULL);
	}

	if (RootTable.role_table)
	{
		JsonbValue	roles;
		HASH_SEQ_STATUS status;
		RoleEntry  *entry;

		roles.type = jbvString;
		roles.val.string.val = "roles";
		roles.val.string.len = strlen(roles.val.string.val);
		pushJsonbValue(&state, WJB_KEY, &roles);
		pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

		hash_seq_init(&status, RootTable.role_table);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
			PushKeyValue(&state, "op", entry->type == Op_Set ? "set" : "del");
			PushKeyValue(&state, "name", entry->name);
			if (entry->password)
			{
#if PG_MAJORVERSION_NUM == 14
				char	   *logdetail;
#else
				const char *logdetail;
#endif
				char	   *encrypted_password;
				PushKeyValue(&state, "password", (char *) entry->password);
				encrypted_password = get_role_password(entry->name, &logdetail);

				if (encrypted_password)
				{
					PushKeyValue(&state, "encrypted_password", encrypted_password);
				}
				else
				{
					elog(ERROR, "Failed to get encrypted password: %s", logdetail);
				}
			}
			if (entry->old_name[0] != '\0')
			{
				PushKeyValue(&state, "old_name", entry->old_name);
			}
			pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		}
		pushJsonbValue(&state, WJB_END_ARRAY, NULL);
	}
	{
		JsonbValue *result = pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		Jsonb	   *jsonb = JsonbValueToJsonb(result);

		return JsonbToCString(NULL, &jsonb->root, 0 /* estimated_len */ );
	}
}

#define ERROR_SIZE 1024

typedef struct
{
	char		str[ERROR_SIZE];
	size_t		size;
} ErrorString;

static size_t
ErrorWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
	/* Docs say size is always 1 */
	ErrorString *str = userdata;

	size_t		to_write = nmemb;

	/* +1 for null terminator */
	if (str->size + nmemb + 1 >= ERROR_SIZE)
		to_write = ERROR_SIZE - str->size - 1;

	/* Ignore everyrthing past the first ERROR_SIZE bytes */
	if (to_write == 0)
		return nmemb;
	memcpy(str->str + str->size, ptr, to_write);
	str->size += to_write;
	str->str[str->size] = '\0';
	return nmemb;
}

static void
SendDeltasToControlPlane()
{
	static CURL		*handle = NULL;

	if (!RootTable.db_table && !RootTable.role_table)
		return;
	if (!ConsoleURL)
	{
		elog(LOG, "ConsoleURL not set, skipping forwarding");
		return;
	}
	if (!ForwardDDL)
		return;

	if (handle == NULL)
	{
		struct curl_slist *headers = NULL;

		headers = curl_slist_append(headers, "Content-Type: application/json");
		if (headers == NULL)
		{
			elog(ERROR, "Failed to set Content-Type header");
		}

		if (jwt_token)
		{
			char		auth_header[8192];

			snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", jwt_token);
			headers = curl_slist_append(headers, auth_header);
			if (headers == NULL)
			{
				elog(ERROR, "Failed to set Authorization header");
			}
		}

		handle = alloc_curl_handle();

		curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "PATCH");
		curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(handle, CURLOPT_URL, ConsoleURL);
		curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, CurlErrorBuf);
		curl_easy_setopt(handle, CURLOPT_TIMEOUT, 3L /* seconds */ );
		curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, ErrorWriteCallback);
	}

	{
		char	   *message = ConstructDeltaMessage();
		ErrorString str;
		const int	num_retries = 5;
		CURLcode	curl_status;
		long		response_code;

		str.size = 0;

		curl_easy_setopt(handle, CURLOPT_POSTFIELDS, message);
		curl_easy_setopt(handle, CURLOPT_WRITEDATA, &str);

		for (int i = 0; i < num_retries; i++)
		{
			if ((curl_status = curl_easy_perform(handle)) == 0)
				break;
			elog(LOG, "Curl request failed on attempt %d: %s", i, CurlErrorBuf);
			pg_usleep(1000 * 1000);
		}
		if (curl_status != CURLE_OK)
			elog(ERROR, "Failed to perform curl request: %s", CurlErrorBuf);

		if (curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code) != CURLE_UNKNOWN_OPTION)
		{
			if (response_code != 200)
			{
				if (str.size != 0)
				{
					elog(ERROR,
						 "Received HTTP code %ld from control plane: %s",
						 response_code,
						 str.str);
				}
				else
				{
					elog(ERROR,
						 "Received HTTP code %ld from control plane",
						 response_code);
				}
			}
		}
	}
}

static void
InitDbTableIfNeeded()
{
	if (!CurrentDdlTable->db_table)
	{
		HASHCTL		db_ctl = {};

		db_ctl.keysize = NAMEDATALEN;
		db_ctl.entrysize = sizeof(DbEntry);
		db_ctl.hcxt = CurTransactionContext;
		CurrentDdlTable->db_table = hash_create(
												"Dbs Created",
												4,
												&db_ctl,
												HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}
}

static void
InitRoleTableIfNeeded()
{
	if (!CurrentDdlTable->role_table)
	{
		HASHCTL		role_ctl = {};

		role_ctl.keysize = NAMEDATALEN;
		role_ctl.entrysize = sizeof(RoleEntry);
		role_ctl.hcxt = CurTransactionContext;
		CurrentDdlTable->role_table = hash_create(
												  "Roles Created",
												  4,
												  &role_ctl,
												  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}
}

static void
PushTable()
{
	DdlHashTable *new_table = MemoryContextAlloc(CurTransactionContext, sizeof(DdlHashTable));

	new_table->prev_table = CurrentDdlTable;
	new_table->role_table = NULL;
	new_table->db_table = NULL;
	CurrentDdlTable = new_table;
}

static void
MergeTable()
{
	DdlHashTable *old_table = CurrentDdlTable;

	CurrentDdlTable = old_table->prev_table;

	if (old_table->db_table)
	{
		DbEntry    *entry;
		HASH_SEQ_STATUS status;

		InitDbTableIfNeeded();

		hash_seq_init(&status, old_table->db_table);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			DbEntry    *to_write = hash_search(
											   CurrentDdlTable->db_table,
											   entry->name,
											   HASH_ENTER,
											   NULL);

			to_write->type = entry->type;
			if (entry->owner != InvalidOid)
				to_write->owner = entry->owner;
			strlcpy(to_write->old_name, entry->old_name, NAMEDATALEN);
			if (entry->old_name[0] != '\0')
			{
				bool		found_old = false;
				DbEntry    *old = hash_search(
											  CurrentDdlTable->db_table,
											  entry->old_name,
											  HASH_FIND,
											  &found_old);

				if (found_old)
				{
					if (old->old_name[0] != '\0')
						strlcpy(to_write->old_name, old->old_name, NAMEDATALEN);
					else
						strlcpy(to_write->old_name, entry->old_name, NAMEDATALEN);
					hash_search(
								CurrentDdlTable->db_table,
								entry->old_name,
								HASH_REMOVE,
								NULL);
				}
			}
		}
		hash_destroy(old_table->db_table);
	}

	if (old_table->role_table)
	{
		RoleEntry  *entry;
		HASH_SEQ_STATUS status;

		InitRoleTableIfNeeded();

		hash_seq_init(&status, old_table->role_table);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			RoleEntry * old;
			bool found_old = false;
			RoleEntry  *to_write = hash_search(
											   CurrentDdlTable->role_table,
											   entry->name,
											   HASH_ENTER,
											   NULL);

			to_write->type = entry->type;
			to_write->password = entry->password;
			strlcpy(to_write->old_name, entry->old_name, NAMEDATALEN);
			if (entry->old_name[0] == '\0')
				continue;

			old = hash_search(
							  CurrentDdlTable->role_table,
							  entry->old_name,
							  HASH_FIND,
							  &found_old);
			if (!found_old)
				continue;
			strlcpy(to_write->old_name, old->old_name, NAMEDATALEN);
			hash_search(CurrentDdlTable->role_table,
						entry->old_name,
						HASH_REMOVE,
						NULL);
		}
		hash_destroy(old_table->role_table);
	}
}

static void
PopTable()
{
	/*
	 * Current table gets freed because it is allocated in aborted
	 * subtransaction's memory context.
	 */
	CurrentDdlTable = CurrentDdlTable->prev_table;
}

static void
NeonSubXactCallback(
					SubXactEvent event,
					SubTransactionId mySubid,
					SubTransactionId parentSubid,
					void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
			return PushTable();
		case SUBXACT_EVENT_COMMIT_SUB:
			return MergeTable();
		case SUBXACT_EVENT_ABORT_SUB:
			return PopTable();
		default:
			return;
	}
}

static void
NeonXactCallback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT)
	{
		SendDeltasToControlPlane();
	}
	RootTable.role_table = NULL;
	RootTable.db_table = NULL;
	Assert(CurrentDdlTable == &RootTable);
}

static bool
RoleIsNeonSuperuser(const char *role_name)
{
	Assert(role_name);

	return strcmp(role_name, "neon_superuser") == 0;
}

static void
HandleCreateDb(CreatedbStmt *stmt)
{
	DefElem    *downer = NULL;
	ListCell   *option;
	bool		found = false;
	DbEntry    *entry;

	InitDbTableIfNeeded();

	foreach(option, stmt->options)
	{
		DefElem    *defel = lfirst(option);

		if (strcmp(defel->defname, "owner") == 0)
			downer = defel;
	}

	entry = hash_search(CurrentDdlTable->db_table,
						stmt->dbname,
						HASH_ENTER,
						&found);
	if (!found)
		memset(entry->old_name, 0, sizeof(entry->old_name));

	entry->type = Op_Set;
	if (downer && downer->arg)
	{
		const char *owner_name = defGetString(downer);

		if (RoleIsNeonSuperuser(owner_name))
			elog(ERROR, "can't create a database with owner neon_superuser");
		entry->owner = get_role_oid(owner_name, false);
	}
	else
	{
		entry->owner = GetUserId();
	}
}

static void
HandleAlterOwner(AlterOwnerStmt *stmt)
{
	const char *name;
	bool		found = false;
	DbEntry    *entry;
	const char *new_owner;

	if (stmt->objectType != OBJECT_DATABASE)
		return;
	InitDbTableIfNeeded();

	name = strVal(stmt->object);
	entry = hash_search(CurrentDdlTable->db_table,
						name,
						HASH_ENTER,
						&found);
	if (!found)
		memset(entry->old_name, 0, sizeof(entry->old_name));

	new_owner = get_rolespec_name(stmt->newowner);
	if (RoleIsNeonSuperuser(new_owner))
		elog(ERROR, "can't alter owner to neon_superuser");
	entry->owner = get_role_oid(new_owner, false);
	entry->type = Op_Set;
}

static void
HandleDbRename(RenameStmt *stmt)
{
	bool		found = false;
	DbEntry    *entry;
	DbEntry    *entry_for_new_name;

	Assert(stmt->renameType == OBJECT_DATABASE);
	InitDbTableIfNeeded();
	entry = hash_search(CurrentDdlTable->db_table,
						stmt->subname,
						HASH_FIND,
						&found);

	entry_for_new_name = hash_search(CurrentDdlTable->db_table,
									 stmt->newname,
									 HASH_ENTER,
									 NULL);
	entry_for_new_name->type = Op_Set;

	if (found)
	{
		if (entry->old_name[0] != '\0')
			strlcpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
		else
			strlcpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
		entry_for_new_name->owner = entry->owner;
		hash_search(CurrentDdlTable->db_table,
					stmt->subname,
					HASH_REMOVE,
					NULL);
	}
	else
	{
		strlcpy(entry_for_new_name->old_name, stmt->subname, NAMEDATALEN);
		entry_for_new_name->owner = InvalidOid;
	}
}

static void
HandleDropDb(DropdbStmt *stmt)
{
	bool		found = false;
	DbEntry    *entry;

	InitDbTableIfNeeded();

	entry = hash_search(CurrentDdlTable->db_table,
						stmt->dbname,
						HASH_ENTER,
						&found);
	entry->type = Op_Delete;
	entry->owner = InvalidOid;
	if (!found)
		memset(entry->old_name, 0, sizeof(entry->old_name));
}

static void
HandleCreateRole(CreateRoleStmt *stmt)
{
	bool		found = false;
	RoleEntry  *entry;
	DefElem    *dpass;
	ListCell   *option;

	InitRoleTableIfNeeded();

	dpass = NULL;
	foreach(option, stmt->options)
	{
		DefElem    *defel = lfirst(option);

		if (strcmp(defel->defname, "password") == 0)
			dpass = defel;
	}

	entry = hash_search(CurrentDdlTable->role_table,
						stmt->role,
						HASH_ENTER,
						&found);
	if (!found)
		memset(entry->old_name, 0, sizeof(entry->old_name));
	if (dpass && dpass->arg)
		entry->password = MemoryContextStrdup(CurTransactionContext, strVal(dpass->arg));
	else
		entry->password = NULL;
	entry->type = Op_Set;
}

static void
HandleAlterRole(AlterRoleStmt *stmt)
{
	char	   *role_name;
	DefElem    *dpass;
	ListCell   *option;
	bool		found = false;
	RoleEntry  *entry;

	InitRoleTableIfNeeded();

	role_name = get_rolespec_name(stmt->role);
	if (RoleIsNeonSuperuser(role_name) && !superuser())
		elog(ERROR, "can't ALTER neon_superuser");

	dpass = NULL;
	foreach(option, stmt->options)
	{
		DefElem    *defel = lfirst(option);

		if (strcmp(defel->defname, "password") == 0)
			dpass = defel;
	}

	/* We only care about updates to the password */
	if (!dpass)
	{
		pfree(role_name);
		return;
	}

	entry = hash_search(CurrentDdlTable->role_table,
						role_name,
						HASH_ENTER,
						&found);
	if (!found)
		memset(entry->old_name, 0, sizeof(entry->old_name));
	if (dpass->arg)
		entry->password = MemoryContextStrdup(CurTransactionContext, strVal(dpass->arg));
	else
		entry->password = NULL;
	entry->type = Op_Set;

	pfree(role_name);
}

static void
HandleRoleRename(RenameStmt *stmt)
{
	bool		found = false;
	RoleEntry  *entry;
	RoleEntry  *entry_for_new_name;

	Assert(stmt->renameType == OBJECT_ROLE);
	InitRoleTableIfNeeded();

	entry = hash_search(CurrentDdlTable->role_table,
						stmt->subname,
						HASH_FIND,
						&found);

	entry_for_new_name = hash_search(CurrentDdlTable->role_table,
									 stmt->newname,
									 HASH_ENTER,
									 NULL);

	entry_for_new_name->type = Op_Set;
	if (found)
	{
		if (entry->old_name[0] != '\0')
			strlcpy(entry_for_new_name->old_name, entry->old_name, NAMEDATALEN);
		else
			strlcpy(entry_for_new_name->old_name, entry->name, NAMEDATALEN);
		entry_for_new_name->password = entry->password;
		hash_search(
					CurrentDdlTable->role_table,
					entry->name,
					HASH_REMOVE,
					NULL);
	}
	else
	{
		strlcpy(entry_for_new_name->old_name, stmt->subname, NAMEDATALEN);
		entry_for_new_name->password = NULL;
	}
}

static void
HandleDropRole(DropRoleStmt *stmt)
{
	ListCell   *item;

	InitRoleTableIfNeeded();

	foreach(item, stmt->roles)
	{
		RoleSpec   *spec = lfirst(item);
		bool		found = false;
		RoleEntry  *entry = hash_search(
										CurrentDdlTable->role_table,
										spec->rolename,
										HASH_ENTER,
										&found);

		entry->type = Op_Delete;
		entry->password = NULL;
		if (!found)
			memset(entry->old_name, 0, sizeof(entry->old_name));
	}
}

static void
HandleRename(RenameStmt *stmt)
{
	if (stmt->renameType == OBJECT_DATABASE)
		return HandleDbRename(stmt);
	else if (stmt->renameType == OBJECT_ROLE)
		return HandleRoleRename(stmt);
}

static void
NeonProcessUtility(
				   PlannedStmt *pstmt,
				   const char *queryString,
				   bool readOnlyTree,
				   ProcessUtilityContext context,
				   ParamListInfo params,
				   QueryEnvironment *queryEnv,
				   DestReceiver *dest,
				   QueryCompletion *qc)
{
	Node	   *parseTree = pstmt->utilityStmt;

	switch (nodeTag(parseTree))
	{
		case T_CreatedbStmt:
			HandleCreateDb(castNode(CreatedbStmt, parseTree));
			break;
		case T_AlterOwnerStmt:
			HandleAlterOwner(castNode(AlterOwnerStmt, parseTree));
			break;
		case T_RenameStmt:
			HandleRename(castNode(RenameStmt, parseTree));
			break;
		case T_DropdbStmt:
			HandleDropDb(castNode(DropdbStmt, parseTree));
			break;
		case T_CreateRoleStmt:
			HandleCreateRole(castNode(CreateRoleStmt, parseTree));
			break;
		case T_AlterRoleStmt:
			HandleAlterRole(castNode(AlterRoleStmt, parseTree));
			break;
		case T_DropRoleStmt:
			HandleDropRole(castNode(DropRoleStmt, parseTree));
			break;
		case T_CreateTableSpaceStmt:
			if (!RegressTestMode)
			{
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("CREATE TABLESPACE is not supported on Neon")));
			}
   			break;
		default:
			break;
	}

	if (PreviousProcessUtilityHook)
	{
		PreviousProcessUtilityHook(
								   pstmt,
								   queryString,
								   readOnlyTree,
								   context,
								   params,
								   queryEnv,
								   dest,
								   qc);
	}
	else
	{
		standard_ProcessUtility(
								pstmt,
								queryString,
								readOnlyTree,
								context,
								params,
								queryEnv,
								dest,
								qc);
	}
}

void
InitControlPlaneConnector()
{
	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = NeonProcessUtility;
	RegisterXactCallback(NeonXactCallback, NULL);
	RegisterSubXactCallback(NeonSubXactCallback, NULL);

	DefineCustomStringVariable(
							   "neon.console_url",
							   "URL of the Neon Console, which will be forwarded changes to dbs and roles",
							   NULL,
							   &ConsoleURL,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomBoolVariable(
							 "neon.forward_ddl",
							 "Controls whether to forward DDL to the control plane",
							 NULL,
							 &ForwardDDL,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(
							 "neon.regress_test_mode",
							 "Controls whether we are running in the regression test mode",
							 NULL,
							 &RegressTestMode,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	jwt_token = getenv("NEON_CONTROL_PLANE_TOKEN");
	if (!jwt_token)
	{
		elog(LOG, "Missing NEON_CONTROL_PLANE_TOKEN environment variable, forwarding will not be authenticated");
	}

}
