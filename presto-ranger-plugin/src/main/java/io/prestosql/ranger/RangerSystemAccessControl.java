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
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import java.net.URL;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final String RANGER_PLUGIN_TYPE = "presto";
    private static final String RANGER_HDFS_AUTHORIZER_IMPL_CLASSNAME = "io.prestosql.ranger.RangerSystemAccessControlImpl";

    private static RangerPluginClassLoader rangerPluginClassLoader;
    private SystemAccessControl rangerSystemAccessControlImpl;

    static class MyClassLoader
            extends RangerPluginClassLoader
    {
        public MyClassLoader(String pluginType, Class<?> pluginClass) throws Exception
        {
            super(pluginType, pluginClass);
            addURL(new URL("file:///Users/davide/repos/prestosql/presto-ranger-plugin/target/presto-ranger-plugin-307-SNAPSHOT.jar"));
        }
    }

    public RangerSystemAccessControl()
    {
        try {
            rangerPluginClassLoader = new MyClassLoader(RANGER_PLUGIN_TYPE, this.getClass());
            Class<SystemAccessControl> cls = (Class<SystemAccessControl>) Class.forName(RANGER_HDFS_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);
            activatePluginClassLoader();

            rangerSystemAccessControlImpl = cls.newInstance();
            System.out.println("YAAAYYYYYYYYYYYYY");
        }
        catch (Exception e) {
            System.out.println("ERRORRRRRRRR");
            System.out.println(e);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        try {
            checkCanSetUser(principal, userName);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denySetUser(principal, userName);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        try {
            checkCanSetSystemSessionProperty(identity, propertyName);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        try {
            checkCanAccessCatalog(identity, catalogName);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        try {
            checkCanCreateSchema(identity, schema);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        try {
            checkCanDropSchema(identity, schema);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        try {
            checkCanRenameSchema(identity, schema, newSchemaName);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        try {
            checkCanShowSchemas(identity, catalogName);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyShowSchemas();
        }
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        try {
            checkCanCreateTable(identity, table);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        try {
            checkCanDropTable(identity, table);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        try {
            checkCanRenameTable(identity, table, newTable);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        try {
            checkCanShowTablesMetadata(identity, schema);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyShowTablesMetadata(schema.getSchemaName());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        try {
            checkCanAddColumn(identity, table);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        try {
            checkCanDropColumn(identity, table);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyDropColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        try {
            checkCanRenameColumn(identity, table);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        try {
            checkCanSelectFromColumns(identity, table, columns);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(), columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        try {
            checkCanInsertIntoTable(identity, table);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        try {
            checkCanDeleteFromTable(identity, table);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        try {
            checkCanCreateView(identity, view);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        try {
            checkCanDropView(identity, view);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        try {
            checkCanCreateViewWithSelectFromColumns(identity, table, columns);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), identity);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        try {
            checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
            activatePluginClassLoader();
        }
        catch (AccessDeniedException e) {
            deactivatePluginClassLoader();
            throw e;
        }
        catch (Exception e) {
            deactivatePluginClassLoader();
            AccessDeniedException.denySetCatalogSessionProperty(catalogName, propertyName);
        }
    }

    private void activatePluginClassLoader()
    {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader()
    {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }
}
