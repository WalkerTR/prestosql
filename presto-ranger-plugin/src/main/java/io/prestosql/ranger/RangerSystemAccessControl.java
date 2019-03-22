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
import org.apache.ranger.plugin.classloader.RangerPluginClassLoaderUtil;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    public RangerSystemAccessControl()
    {
        try {
            RangerPluginClassLoaderUtil clUtil = RangerPluginClassLoaderUtil.getInstance();
            System.out.println("PLUGIN FILES");
            URI uri = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI();
            Path path = Paths.get(URI.create(uri.toString()));
            System.out.println(uri.toString());
            System.out.println(path.getParent().toString() + File.separatorChar + "ranger-%-plugin-impl".replaceAll("%", RANGER_PLUGIN_TYPE));
            for (URL url : clUtil.getPluginFilesForServiceTypeAndPluginclass(RANGER_PLUGIN_TYPE, this.getClass())) {
                System.out.println(url.toString());
            }

            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());
            Class<SystemAccessControl> cls = (Class<SystemAccessControl>) Class.forName(RANGER_HDFS_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);
            activatePluginClassLoader();
            System.out.println(Thread.currentThread().getContextClassLoader().toString());
            rangerSystemAccessControlImpl = cls.newInstance();
            System.out.println("YAAAYYYYYYYYYYYYY");
        }
        catch (Exception e) {
            System.out.println("ERRORRRRRRRR");
            System.out.println(e);
            throw new RuntimeException(e);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        try {
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanSetUser(principal, userName);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanSetSystemSessionProperty(identity, propertyName);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanAccessCatalog(identity, catalogName);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanCreateSchema(identity, schema);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanDropSchema(identity, schema);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanRenameSchema(identity, schema, newSchemaName);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanShowSchemas(identity, catalogName);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanCreateTable(identity, table);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanDropTable(identity, table);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanRenameTable(identity, table, newTable);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanShowTablesMetadata(identity, schema);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanAddColumn(identity, table);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanDropColumn(identity, table);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanRenameColumn(identity, table);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanSelectFromColumns(identity, table, columns);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanInsertIntoTable(identity, table);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanDeleteFromTable(identity, table);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanCreateView(identity, view);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanDropView(identity, view);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanCreateViewWithSelectFromColumns(identity, table, columns);
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
            activatePluginClassLoader();
            rangerSystemAccessControlImpl.checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
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
