package com.lucidworks.apollo.pipeline.index.stages.transform;

import com.lucidworks.apollo.common.pipeline.PipelineDocument;
import com.lucidworks.apollo.common.pipeline.PipelineField;
import com.lucidworks.apollo.pipeline.PipelineCollector;
import com.lucidworks.apollo.pipeline.PipelineContext;
import com.lucidworks.apollo.pipeline.StageCallback;
import com.lucidworks.apollo.pipeline.index.IndexStage;
import com.lucidworks.apollo.pipeline.stages.Stage;

import com.google.inject.Inject;
import com.lucidworks.apollo.component.ResourceLoader;

import com.lucidworks.apollo.modinfodesigns.BlobStoreObjectFactory;
import com.lucidworks.apollo.pipeline.index.transform.config.RichObjectParserConfig;
import com.lucidworks.apollo.pipeline.index.transform.config.RichObjectParserConfig.FieldMapping;
import com.lucidworks.apollo.pipeline.index.transform.config.RichObjectParserConfig.InnerMapping;
import com.lucidworks.apollo.pipeline.index.transform.config.RichObjectParserConfig.Format;
import com.lucidworks.apollo.pipeline.index.transform.config.RichObjectParserConfig.Mode;

import com.modinfodesigns.app.ApplicationManager;
import com.modinfodesigns.app.IObjectFactory;
import com.modinfodesigns.app.ModInfoObjectFactory;
import com.modinfodesigns.utils.FileMethods;
import com.modinfodesigns.property.transform.IPropertyHolderTransform;
import com.modinfodesigns.property.transform.PropertyTransformException;
import com.modinfodesigns.property.IProperty;
import com.modinfodesigns.property.IDataObjectBuilder;
import com.modinfodesigns.property.DataObject;
import com.modinfodesigns.property.PropertyList;
import com.modinfodesigns.property.IntrinsicPropertyDelegate;

import com.modinfodesigns.property.transform.json.JSONParserTransform;
import com.modinfodesigns.property.transform.xml.XMLParserTransform;

import com.modinfodesigns.utils.FileMethods;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

import java.io.StringWriter;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates JSON string into PipelineDocument fields which can be simple field/value pairs or nested objects.
 *
 * Uses Modular Informatic Designs toolkit to do initial parsing. The toolkit creates an instance 
 * of a com.modinfodesigns.property.DataObject class that can be mapped with object paths like /parent/child/grandchild/property_name
 * Paths can point to primitive properties like Strings or numbers (Integer) or to nested DataObjects.
 *
 * If there are nested DataObjects once the transformation has concluded, these can either be emitted separately
 * OR they can be nested as nested PipelineDocuments to do block joins, etc.
 */

@Stage
public class RichObjectParserStage extends IndexStage<RichObjectParserConfig> {
    
  private transient static final Logger LOG = LoggerFactory.getLogger( RichObjectParserStage.class );

  private static String DATA_TRANSFORM = "DataTransform";
  private static String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
  private static int STRING_MAX = 256;

  private ResourceLoader resourceLoader;
    
  private String defaultFloat = "float";  // or "double"
    
  private boolean sendAllDocs = true;
    
  private static int nReceived = 0;
  private static int nProcessed = 0;
  private static int nFailed = 0;
    
    
  @Inject
  public RichObjectParserStage( ResourceLoader resourceLoader ) {
    this.resourceLoader = resourceLoader;
  }

  @Override
  public void process( PipelineDocument pipelineDoc, PipelineContext pipelineContext, RichObjectParserConfig config,
                       PipelineCollector<PipelineDocument> collector, StageCallback<PipelineDocument> callback )
                       throws Exception {
    // LOG.debug( "RichObjectParser.process( ) ..." );
    long startTime = System.currentTimeMillis( );
                           
    String inputField = config.getInputField( );
    Format format = config.getFormat( );
                           
    String parentIDFieldName = config.getParentIDFieldName( );
                           
    // LOG.debug( "parsing inputField " + inputField + " format = '" + format.toString( ) + "'" );
                           
    String saveFailedRecordsToPath = config.getFailedRecordsPath( );
 
    String dataString = getDataString( inputField, pipelineDoc );
    pipelineDoc.removeFields( inputField );
                           
    try {
      IDataObjectBuilder dobjBuilder = getDataObjectBuilder( format.toString().toUpperCase( ) );
                           
      // LOG.debug( "Got data string: '" + dataString + "'" );
      boolean processed = false;
      if (dataString != null) {
        DataObject dataObj = dobjBuilder.createDataObject( dataString );
        // LOG.debug( "Got DataObject: " + dataObj.getValue( IProperty.XML_FORMAT ) );
        long parseTime = System.currentTimeMillis( );
        LOG.info( "parsing time was " + (parseTime  - startTime) + " milliseconds" );
        
        boolean hasTransformErrors = false;
        String transformErrors = "";
        List<IPropertyHolderTransform> pTransforms = getTransforms( config );
        if (pTransforms != null) {
          // LOG.debug( "Applying Property Transforms " );
          for (IPropertyHolderTransform pTransform : pTransforms ) {
            // LOG.debug( "Applying PropertyTransform: " + pTransform );
            try {
              dataObj = (DataObject)pTransform.transformPropertyHolder( dataObj );
            }
            catch ( PropertyTransformException pte ) {
              LOG.error( "Got PropertyTransformException!!! " + pte.getMessage( ) );
              if (transformErrors == null) {
                transformErrors = pte.getClass().getName( ) + ": " + pte.getMessage( );;
              }
              else {
                transformErrors = transformErrors + "; " + pte.getClass().getName( ) + ": " + pte.getMessage( );
              }

              hasTransformErrors = true;
            }
          }
        }

        long transformTime = System.currentTimeMillis( );
        LOG.info( "Transform time was " + (transformTime - parseTime )+ " milliseconds" );
          
        if (hasTransformErrors) {
          // LOG.debug( "Had Transform Errors");
          fileFailedDoc( pipelineDoc, dataString, "Had Transform Errors: " + transformErrors, saveFailedRecordsToPath );
        }
    
        int nLinkedDocs = 0;
          
        HashSet<String> mappedFields = new HashSet<String>( );
          
        // LOG.debug( "Adding Dynamic Properties ... " );
        Iterator<IProperty> props = dataObj.getProperties( );
        while ( props != null && props.hasNext( ) ) {
          IProperty prop = props.next( );
          if (!(prop instanceof IntrinsicPropertyDelegate) && !mappedFields.contains( prop.getName( ))) {
            addDynamicField( pipelineDoc, prop );
          }
        }
          
        // LOG.debug( "Processing fieldMappings ... " );
        List<FieldMapping> fieldMappings = config.getFieldMappings( );
        for (FieldMapping fieldMapping : fieldMappings ) {
          IProperty prop = dataObj.getProperty( fieldMapping.inputPath );
          if (prop != null) {
            String mode = fieldMapping.mode.toString( );
            // LOG.debug( "Mode = '" + mode + "'" );
            if ( mode.equals( "field" ) ) {
              addField( pipelineDoc, prop, fieldMapping.solrField );
              mappedFields.add( fieldMapping.inputPath );
            }
            else if (mode.equals( "linked_object" )) {
              // LOG.debug( "linked object is a " + prop.getClass().getName() );
              if (prop instanceof PropertyList ) {
                Iterator<IProperty> propIt = ((PropertyList)prop).getProperties( );
                while ( propIt.hasNext( ) ) {
                  IProperty pr = propIt.next( );
                  if (pr instanceof DataObject )
                  {
                    PipelineDocument pDoc = createPipelineDocument( pipelineDoc, (DataObject)pr, parentIDFieldName,
                                                                    fieldMapping.parentIDField, fieldMapping.innerMappings, fieldMapping.copyParentFields );
                    pDoc.setId( pipelineDoc.getId( ) + "#" + Integer.toString( nLinkedDocs++ ) );
                    if (fieldMapping.solrField != null) {
                      pDoc.addField( fieldMapping.solrField, ((DataObject)pr).getName( ) );
                    }
                    collector.write( pDoc );
                  }
                }
                // LOG.debug( "linked_object DONE" );
              }
              else if (prop instanceof DataObject ) {
                PipelineDocument pDoc = createPipelineDocument( pipelineDoc, (DataObject)prop, parentIDFieldName,
                                                                fieldMapping.parentIDField, fieldMapping.innerMappings, fieldMapping.copyParentFields );
                pDoc.setId( pipelineDoc.getId( ) + "#" + Integer.toString( nLinkedDocs++ ) );
                if ( fieldMapping.solrField != null ) {
                  pDoc.addField( fieldMapping.solrField, ((DataObject)prop).getName( ) );
                }
                collector.write( pDoc );
              }
            }
            else if (mode.equals( "nested_object" )) {
              if (prop instanceof PropertyList ) {
                Iterator<IProperty> propIt = ((PropertyList)prop).getProperties( );
                while ( propIt.hasNext( ) ) {
                  IProperty pr = propIt.next( );
                  if (pr instanceof DataObject )
                  {
                    PipelineDocument pDoc = createPipelineDocument( pipelineDoc, (DataObject)pr, parentIDFieldName,
                                                                    fieldMapping.parentIDField, fieldMapping.innerMappings, fieldMapping.copyParentFields );
                    pipelineDoc.addField( fieldMapping.solrField, pDoc );
                  }
                }
              }
              else if (prop instanceof DataObject ) {
                PipelineDocument pDoc = createPipelineDocument( pipelineDoc, (DataObject)prop, parentIDFieldName,
                                                                fieldMapping.parentIDField, fieldMapping.innerMappings, fieldMapping.copyParentFields );
                pipelineDoc.addField( fieldMapping.solrField, pDoc );
              }
              mappedFields.add( fieldMapping.inputPath );
            }
            else if (mode.equals("json_string" )) {
              // LOG.debug( "adding JSON string field: " + fieldMapping.solrField + " = " + prop.getValue( IProperty.JSON_FORMAT ) );
              pipelineDoc.addField( fieldMapping.solrField, prop.getValue( IProperty.JSON_FORMAT ));
              mappedFields.add( fieldMapping.inputPath );
            }
          }
        }

        dataObj.removeProperties( );
        dataObj = null;

        processed = true;
      }
      else {
        LOG.error( "Data String field was null! Cannot process doc!" );
      }
                           
      // LOG.debug( "process  DONE - writing pipelineDoc" );
      
      if ( processed || sendAllDocs ) {
        collector.write( pipelineDoc );
      }
        
      long dur = System.currentTimeMillis( ) - startTime;
      LOG.info( "RichObjectParser  Processing " + pipelineDoc.getId( ) + ": " + dataString.length() + " took " + dur + " (millis)" );
    }
    catch ( Throwable e ) {
      ++nFailed;
      // LOG.debug( "Failed:\n" + e.getClass( ).getName( ) + ": " + e.getMessage( ) );
        
      StringWriter sw = new StringWriter( );
      PrintWriter pw = new PrintWriter( sw );
      e.printStackTrace( pw );
      LOG.debug( sw.toString( ) );
        
      fileFailedDoc(  pipelineDoc, dataString, e.getClass().getName( ) + "\n" + sw.toString( ), saveFailedRecordsToPath );
        
      if (e instanceof Error) throw (Error)e;
    }
  }

  private IDataObjectBuilder getDataObjectBuilder( String format ) throws Exception {
    if (format.equalsIgnoreCase( "JSON" )) {
      return new JSONParserTransform( );
    }
    else if (format.equalsIgnoreCase( "XML" )) {
      return new XMLParserTransform( );
    }
      
    LOG.error( "ERROR: Could not find parser for " + format );
    throw new Exception( "No parser for '" + format + "'" );
  }
    
  private PipelineDocument createPipelineDocument( PipelineDocument parent, DataObject dobj, String parentIDFieldName,
                                                  String parentIDField, List<InnerMapping> fieldMappings, List<String> copyParentFields ) {
    // LOG.debug( "createPipelineDocument: " + dobj.getValue( IProperty.XML_FORMAT ));
    PipelineDocument pDoc = new PipelineDocument( );
    if (parentIDField != null) {
      PipelineField parentField = parent.getFirstField( parentIDField );
      // get the parent ID Field value from the parent ID Field
      if (parentField != null) {
        pDoc.addField( parentIDFieldName, parentField.getValue( ).toString( ) );
      }
      else {
        LOG.error( "Could not add parent field: " + parentIDField );
      }
    }
     
    // LOG.debug( "Checking mapped Fields ..." );
    HashSet<String> mappedFields = new HashSet<String>( );
    if (fieldMappings != null) {
      for ( InnerMapping fieldMapping : fieldMappings ) {
        IProperty prop = dobj.getProperty( fieldMapping.inputPath );
        if (prop != null) {
          addField( pDoc, prop, fieldMapping.solrField );
          mappedFields.add( fieldMapping.inputPath );
        }
      }
    }
    
    // LOG.debug( "adding Dynamic Properties" );
    Iterator<IProperty> props = dobj.getProperties( );
    while ( props != null && props.hasNext( ) ) {
      IProperty prop = props.next( );
      if (!(prop instanceof IntrinsicPropertyDelegate) && !mappedFields.contains( prop.getName( ))) {
        addDynamicField( pDoc, prop );
      }
    }
      
    if (copyParentFields != null) {
      for (String parentField : copyParentFields ) {
        List<PipelineField> pFields = parent.getFields( parentField );
        if (pFields != null) {
          for (PipelineField field : pFields ) {
            pDoc.addField( new PipelineField( field.getName(), field.getValue() ) ); // copy this
          }
        }
      }
    }
      
    // LOG.debug( "createPipelineDocument DONE!" );
    return pDoc;
  }
    
  private void addDynamicField( PipelineDocument pDoc, IProperty prop ) {
    if (prop == null) return;
    // LOG.debug( "addDynamicField " + prop.getClass().getName( ) );
    // -------------------------------------------------------------
    // check property type and add dynamic suffixes
    // if instance of PropertyList - make it multiValue in Solr
    //   if type == StringProperty ... etc.
    // -------------------------------------------------------------
    String suffix = "";
    boolean multiValue = false;
    if (prop instanceof PropertyList) multiValue = true;
    String propType = getType( prop );
        
    // LOG.debug( "propType = '" + propType + "'" );
    if (propType.equals( "com.modinfodesigns.property.quantity.IntegerProperty")) {
      suffix = (multiValue) ? "_is" : "_i";
    }
    else if (propType.equals( "com.modinfodesigns.property.quantity.ScalarQuantity" )) {
      suffix = (multiValue) ? ((defaultFloat.equals( "float" )) ? "_fs" : "_ds") : ((defaultFloat.equals( "float" )) ? "_f" : "_d");
    }
    else if (propType.equals( "com.modinfodesigns.property.time.DateProperty" )) {
      suffix = (multiValue) ? "_dts" : "_dt";
    }
    else if (propType.equals( "com.modinfodesigns.property.BooleanProperty" )) {
      suffix = (multiValue) ? "_bs" : "_b";
    }
    else if (propType.equals( "com.modinfodesigns.property.string.TextProperty" )) {
      suffix = (multiValue) ? "_t" : "_txt";
    }
    else {
        String val = getValue( prop );
        if ( val.length() < STRING_MAX ) {
            suffix = (multiValue) ? "_ss" : "_s";
        }
        else {
            suffix = (multiValue) ? "_txt" : "_t";
        }
    }
      
    String propName = new String( prop.getName( ) + suffix );
    // LOG.debug( "adding dynamic property: " + propName );
    addField( pDoc, prop, propName );
 }
    
  private String getType( IProperty prop ) {
    // LOG.debug( "getType of " + prop.getType( ) );
    if (prop instanceof PropertyList) {
      // first property in list defines it - could check if heterogeneous in which case type is String
      Iterator<IProperty> propIt = ((PropertyList)prop).getProperties( );
      String firstType = null;
      while( propIt != null && propIt.hasNext( ) ) {
        IProperty childProp = propIt.next( );
        if (firstType == null) firstType = childProp.getType( );
        else {
          if (!childProp.getType().equals( firstType )) {
            return "com.modinfodesigns.property.string.StringProperty";
          }
        }
      }

      return (firstType != null) ? firstType : "com.modinfodesigns.property.string.StringProperty";
    }
    else {
      return (prop.getType( ) != null) ? prop.getType( ) : "com.modinfodesigns.property.string.StringProperty";
    }
  }
    
  private String getValue( IProperty prop ) {
    if (getType( prop ).equals( "com.modinfodesigns.property.time.DateProperty" )) {
      return prop.getValue( SOLR_DATE_FORMAT );
    }
      
    return prop.getValue( );
  }
    
  private void addField( PipelineDocument pDoc, IProperty prop, String fieldName ) {
    if (prop instanceof PropertyList) {
      PropertyList pList = (PropertyList)prop;
      Iterator<IProperty> props = pList.getProperties( );
      while (props != null && props.hasNext() ) {
        IProperty pr = props.next( );
        // LOG.debug( "List addField( " + fieldName + ", '" + pr.getValue( ) + "' )" );
        
        pDoc.addField( fieldName, getValue( pr ) );
      }
    }
    else {
      // LOG.debug( "Single addField( " + fieldName + ", '" + prop.getValue( ) + " type: " + prop.getType( ) + "' )" );
      pDoc.addField( fieldName, getValue( prop ) );
    }
  }
    
  private String getDataString( String dataField, PipelineDocument pipelineDoc ) {
    // LOG.debug( "getDataString from " + dataField );
    PipelineField field = pipelineDoc.getFirstField( dataField );
    return (field != null && field.getValue() != null ) ? field.getValue( ).toString( ) : null;
  }


  private List<IPropertyHolderTransform> getTransforms( RichObjectParserConfig config ) throws Exception {
    initObjectFactory( config );
    ApplicationManager appMan = ApplicationManager.getInstance( );
      
    List<Object> appObjects = appMan.getApplicationObjects( DATA_TRANSFORM );
    ArrayList<IPropertyHolderTransform> pTransforms = new ArrayList<IPropertyHolderTransform>( );
    for (Object appObject : appObjects ) {
      if (appObject instanceof IPropertyHolderTransform ) {
        pTransforms.add( (IPropertyHolderTransform)appObject );
      }
    }
      
    return pTransforms;
  }
    

  private void initObjectFactory( RichObjectParserConfig config ) {
      
    // LOG.debug( "initObjectFactory" );
    String configFile = config.getDataObjectTransform( );
    if (configFile == null)
    {
      LOG.error( "Config file is NULL!" );
      return;
    }
     
    // LOG.debug( "initializing object factory: " + configFile );
    ApplicationManager appMan = ApplicationManager.getInstance( );
    if (appMan.getObjectFactory( configFile ) == null) {
      synchronized( this ) {
        IObjectFactory objectFac = null;
        
        // check if configFile is has a file path or just a file name - if path, use ModInfoObjectFactory
        // if a filename - use BlobStoreObjectFactory
        if (configFile.startsWith( "/" )) {
          // read file with FileMethods
          String configXML = FileMethods.readFile( configFile );
          // LOG.debug( "Initializing ModInfoObjectFactory with " + configXML );
          objectFac = new ModInfoObjectFactory( );
          objectFac.initialize( configXML );
        }
        else {
          objectFac = new BlobStoreObjectFactory( this.resourceLoader );
          objectFac.initialize( configFile );
        }
          
        appMan.addObjectFactory( configFile, objectFac );
      }
    }
  }
    
  private void fileFailedDoc( PipelineDocument pipelineDoc, String dataString, String failedMessage, String saveFailedRecordsToPath ) {
    // LOG.debug( "fileFailedDoc: " + failedMessage );
    // save dataString to a file!
    if (saveFailedRecordsToPath != null ) {
      // get the file name from the solr doc ...
      PipelineField pField = pipelineDoc.getFirstField( "id" );
      if (pField != null && pField.getValue() != null) {
        String id = pField.getValue( ).toString( );
        String fileName =  ( id.indexOf( "/" ) < 0 ) ? id : id.substring( id.lastIndexOf( "/" ) + 1 );
                
        String errorList = saveFailedRecordsToPath + "/errors.txt";
        String errorLine = Integer.toString( nFailed ) + " of " + Integer.toString( nReceived ) + ") " + id + ": " + failedMessage;
        FileMethods.addToFile( errorList, errorLine );
                
        fileName = saveFailedRecordsToPath + "/" + fileName;
        FileMethods.writeFile( fileName, dataString );
      }
    }
  }


    
  @Override
  public Class<RichObjectParserConfig> getStageConfigClass() {
    return RichObjectParserConfig.class;
  }
}
