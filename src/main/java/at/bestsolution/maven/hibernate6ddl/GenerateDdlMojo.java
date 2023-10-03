/*
 * Copyright (C) 2017 Jens Pelzetter
 * Copyright (C) 2023 BestSolution.at
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package at.bestsolution.maven.hibernate6ddl;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.hibernate.HibernateException;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SimpleDatabaseVersion;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfoSource;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.schema.TargetType;

import com.google.common.base.Objects;

@Mojo( name = "gen-ddl", defaultPhase = LifecyclePhase.PROCESS_RESOURCES, requiresDependencyCollection = ResolutionScope.COMPILE_PLUS_RUNTIME, threadSafe = true )
public class GenerateDdlMojo extends AbstractMojo {

	/**
	 * Location of the output file.
	 */
	@Parameter( defaultValue = "${project.build.directory}/generated-resources/sql/ddl" )
	private File outputDirectory;

	@Parameter( required = false )
	private String[] packages;

	/**
	 * Set to {@code true} to include classes in {@code src/test}.
	 */
	@Parameter( defaultValue = "false" )
	private boolean includeTestClasses;

	/**
	 * Dialect (class name) to generate DDL for, without the `Dialect` suffix. May be suffixed with
	 * `@x` to select a specific major version. Example: `PostgreSQL@13` selects 
	 * `org.hibernate.dialect.PostgreSQLDialect` with major version set to 13.
	 * @see https://docs.jboss.org/hibernate/stable/orm/javadocs/org/hibernate/dialect/package-summary.html
	 */
	@Parameter( required = true )
	private String[] dialects;

	@Parameter( defaultValue = "false" )
	private boolean createDropStatements;

	@Parameter( defaultValue = "${project}", readonly = true )
	private transient MavenProject project;

	@Override
	public void execute() throws MojoExecutionException, MojoFailureException {
		final File outputDir = outputDirectory;

		getLog().info( String.format( "Generating DDL SQL files in %s.", outputDir.getAbsolutePath() ) );

		if( !outputDir.exists() && !outputDir.mkdirs() ) {
			throw new MojoFailureException( "Failed to create output directory for SQL DDL files." );
		}

		final Set<Class<?>> entityClasses;
		final Set<Package> annotatedPackages;
		if( packages == null || packages.length == 0 ) {
			final EntityFinder entityFinder = EntityFinder.forClassPath(
					project, getLog(), includeTestClasses );
			entityClasses = entityFinder.findEntities();
			annotatedPackages = entityFinder.findPackages();
		} else {
			// Find the entity classes in the packages.
			entityClasses = new HashSet<>();
			annotatedPackages = new HashSet<>();
			for( final String packageName : packages ) {
				final EntityFinder entityFinder = EntityFinder.forPackage(
						project, getLog(), packageName, includeTestClasses );
				final Set<Class<?>> packageEntities = entityFinder
						.findEntities();
				final Set<Package> packagesWithAnnotations = entityFinder
						.findPackages();
				entityClasses.addAll( packageEntities );
				annotatedPackages.addAll( packagesWithAnnotations );
			}
		}

		getLog().info(
				String.format(
						"Found %d entities.", entityClasses.size() ) );
		if( annotatedPackages != null && !annotatedPackages.isEmpty() ) {
			getLog().info(
					String.format(
							"Found %d annotated packages.", annotatedPackages.size() ) );
		}

		for( final String dialect : dialects ) {
			generateDdl( dialect, annotatedPackages, entityClasses );
		}
	}

	public void generateDdl( final String dialectName, final Set<Package> packages, final Set<Class<?>> entityClasses ) {
		final var registryBuilder = new StandardServiceRegistryBuilder();
		registryBuilder.addService( DialectFactory.class, new DialectFactory() {
			private static final long serialVersionUID = 1L;

			@Override
			public Dialect buildDialect( Map<String, Object> configValues, DialectResolutionInfoSource resolutionInfoSource ) throws HibernateException {
				String[] dialectParts = dialectName.split( "@", 2 );
				String dialectClassName = String.format( "org.hibernate.dialect.%sDialect", dialectParts[0] ); 
				try {
					Class<Dialect> dialectClass = (Class<Dialect>) Class.forName( dialectClassName );
					if (dialectParts.length > 1) {
						DatabaseVersion version = new SimpleDatabaseVersion( Integer.parseInt( dialectParts[1] ), 0 );
						return dialectClass.getConstructor( DatabaseVersion.class ).newInstance( version );
					} else {
						return dialectClass.getConstructor().newInstance();
					}
				} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
					throw new HibernateException( String.format( "Could not instantiate named dialect class [%s]", dialectClassName ), e );
				}
			}
		} );
		registryBuilder.applySetting( "hibernate.hbm2ddl.auto", createDropStatements ? "create-drop" : "create" );
		registryBuilder.applySetting( "hibernate.use_sql_comments", true );
		
		final StandardServiceRegistry standardRegistry = registryBuilder.build();
        final MetadataSources metadataSources = new MetadataSources(standardRegistry);
        
        for (final Package aPackage : packages) {
            metadataSources.addPackage(aPackage);
        }
        for (final Class<?> entityClass : entityClasses) {
            metadataSources.addAnnotatedClass(entityClass);
        }

        final Metadata metadata = metadataSources.buildMetadata();
        
        final SchemaExport export = new SchemaExport();
        export.setDelimiter(";");
        export.setManageNamespaces(true);
        export.setHaltOnError(true);
        export.setFormat(true);
        export.setOutputFile( outputDirectory.toPath().resolve( dialectName.replace( "@", "" ).toLowerCase() + ".sql" ).toString() );
        export.setOverrideOutputFileContent();
        export.execute(EnumSet.of(TargetType.SCRIPT), createDropStatements ? SchemaExport.Action.BOTH : SchemaExport.Action.CREATE, metadata);
	}

}
