/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.ranger;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SystemAccessControl;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

public class RangerSystemAccessControlImpl
        implements SystemAccessControl
{
    private RangerBasePlugin rangerPlugin;

    public RangerSystemAccessControlImpl()
    {
        rangerPlugin = new RangerBasePlugin("presto", "presto");
        rangerPlugin.init();
        System.out.println("INITIALIZED!!!!!!!!!!!!!!!!!!");
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        AccessDeniedException.denySetUser(principal, userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        AccessDeniedException.denySetSystemSessionProperty(propertyName);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        AccessDeniedException.denyCatalogAccess(catalogName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        AccessDeniedException.denyCreateSchema(schema.getSchemaName());
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        AccessDeniedException.denyDropSchema(schema.getSchemaName());
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        AccessDeniedException.denyRenameSchema(schema.getSchemaName(), newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        AccessDeniedException.denyShowSchemas();
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        AccessDeniedException.denyCreateTable(table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        AccessDeniedException.denyDropTable(table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        AccessDeniedException.denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        AccessDeniedException.denyShowTablesMetadata(schema.getSchemaName());
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        AccessDeniedException.denyDropColumn(table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        AccessDeniedException.denyRenameColumn(table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(), columns);
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        AccessDeniedException.denyInsertTable(table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        AccessDeniedException.denyDeleteTable(table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        AccessDeniedException.denyCreateView(view.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        AccessDeniedException.denyDropView(view.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        AccessDeniedException.denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), identity);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        AccessDeniedException.denySetCatalogSessionProperty(catalogName, propertyName);
    }
}
