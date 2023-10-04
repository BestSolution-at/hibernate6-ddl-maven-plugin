package at.bestsolution.maven.hibernate6ddl;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.schema.SourceType;
import org.hibernate.tool.schema.internal.DefaultSchemaFilterProvider;
import org.hibernate.tool.schema.internal.ExceptionHandlerCollectingImpl;
import org.hibernate.tool.schema.internal.ExceptionHandlerHaltImpl;
import org.hibernate.tool.schema.spi.ContributableMatcher;
import org.hibernate.tool.schema.spi.ExceptionHandler;
import org.hibernate.tool.schema.spi.ExecutionOptions;
import org.hibernate.tool.schema.spi.SchemaFilter;
import org.hibernate.tool.schema.spi.SchemaFilterProvider;
import org.hibernate.tool.schema.spi.SchemaManagementTool;
import org.hibernate.tool.schema.spi.SchemaManagementToolCoordinator;
import org.hibernate.tool.schema.spi.ScriptSourceInput;
import org.hibernate.tool.schema.spi.SourceDescriptor;
import org.hibernate.tool.schema.spi.TargetDescriptor;

/**
 * "Patch" for the {@link SchemaExport} class, as the original version ignores
 * any {@link SchemaFilter} and always uses the default implementation. This class
 * overrides
 */
public class FilteredSchemaExport extends SchemaExport {
	
	// fields from parent are not accessible but needed in "doExecution", hence copied into the subclass
	private boolean haltOnError = false;
	private boolean format = false;
	private String delimiter = null;
	private String importFiles;
	
	@Override
	public SchemaExport setImportFiles(String importFiles) {
		this.importFiles = importFiles;
		super.setImportFiles( importFiles );
		return this;
	}

	@Override
	public SchemaExport setDelimiter(String delimiter) {
		this.delimiter = delimiter;
		super.setDelimiter( delimiter );
		return this;
	}

	@Override
	public SchemaExport setFormat(boolean format) {
		this.format = format;
		super.setFormat( format );
		return this;
	}

	@Override
	public SchemaExport setHaltOnError(boolean haltOnError) {
		this.haltOnError = haltOnError;
		super.setHaltOnError( haltOnError );
		return this;
	}
	
	@Override
	public void doExecution( Action action, boolean needsJdbc, Metadata metadata, ServiceRegistry serviceRegistry, TargetDescriptor targetDescriptor ) {
		// mostly copied from the parent class, only difference is the creation and usage of the SchemaFilterProvider if one is configured
		
		Map<String,Object> config = new HashMap<>( serviceRegistry.getService( ConfigurationService.class ).getSettings() );
		config.put( AvailableSettings.HBM2DDL_DELIMITER, delimiter );
		config.put( AvailableSettings.FORMAT_SQL, format );
		config.put( AvailableSettings.HBM2DDL_IMPORT_FILES, importFiles );

		final SchemaManagementTool tool = serviceRegistry.getService( SchemaManagementTool.class );

		final ExceptionHandler exceptionHandler = haltOnError
				? ExceptionHandlerHaltImpl.INSTANCE
				: new ExceptionHandlerCollectingImpl();
		final Object filterProviderOption = config.get( AvailableSettings.HBM2DDL_FILTER_PROVIDER );
		final SchemaFilterProvider schemaFilter = serviceRegistry.getService( StrategySelector.class ).resolveDefaultableStrategy(
				SchemaFilterProvider.class,
				filterProviderOption,
				DefaultSchemaFilterProvider.INSTANCE
		);

		final SourceDescriptor sourceDescriptor = new SourceDescriptor() {
			@Override
			public SourceType getSourceType() {
				return SourceType.METADATA;
			}

			@Override
			public ScriptSourceInput getScriptSourceInput() {
				return null;
			}
		};

		try {
			if ( action.doDrop() ) {
				final ExecutionOptions executionOptions = SchemaManagementToolCoordinator.buildExecutionOptions(
						config,
						schemaFilter.getDropFilter(),
						exceptionHandler
				);
				tool.getSchemaDropper( config ).doDrop(
						metadata,
						executionOptions,
						ContributableMatcher.ALL,
						sourceDescriptor,
						targetDescriptor
				);
			}

			if ( action.doCreate() ) {
				final ExecutionOptions executionOptions = SchemaManagementToolCoordinator.buildExecutionOptions(
						config,
						schemaFilter.getCreateFilter(),
						exceptionHandler
				);
				tool.getSchemaCreator( config ).doCreation(
						metadata,
						executionOptions,
						ContributableMatcher.ALL,
						sourceDescriptor,
						targetDescriptor
				);
			}
		}
		finally {
			if ( exceptionHandler instanceof ExceptionHandlerCollectingImpl ) {
				getExceptions().addAll( ( (ExceptionHandlerCollectingImpl) exceptionHandler ).getExceptions() );
			}
		}
	}
}
