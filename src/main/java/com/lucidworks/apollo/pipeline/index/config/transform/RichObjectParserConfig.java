package com.lucidworks.apollo.pipeline.index.transform.config;

import com.lucidworks.apollo.pipeline.StageConfig;
import com.lucidworks.apollo.pipeline.schema.Annotations.Schema;
import com.lucidworks.apollo.pipeline.schema.Annotations.SchemaProperty;
import com.lucidworks.apollo.pipeline.schema.Annotations.StringProperty;
import com.lucidworks.apollo.pipeline.schema.Annotations.BooleanProperty;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

import java.util.ArrayList;
import java.util.List;

@JsonTypeName("rich-object-parser")
@Schema(
  type = "rich-object-parser",
  title = "Rich Object Parser",
  description = "Parses JSON or XML to stuctured data objects"
)
public class RichObjectParserConfig extends StageConfig {

  // to do: make this an enum of JSON XML
  @SchemaProperty( title="Input Format" )
  private final Format format;
    
  @SchemaProperty( title="input field" )
  private final String inputField;
    
  @SchemaProperty( title="Parent ID Field Name" )
  private final String parentIDFieldName;
    
  @SchemaProperty(title="Data Object Transform Config")
  private final String dataObjectTransform;
    
  @SchemaProperty(title="Field Mappings" )
  private final List<FieldMapping> fieldMappings;
    

  @JsonCreator
  protected RichObjectParserConfig( @JsonProperty("id") String id,
                                    @JsonProperty("format") Format format,
                                    @JsonProperty("inputField") String inputField,
                                    @JsonProperty("parentIDFieldName") String parentIDFieldName,
                                    @JsonProperty("dataObjectTransform") String dataObjectTransform,
                                    @JsonProperty("fieldMappings") List<FieldMapping> fieldMappings ) {
    super(id);
    this.format = format;
    this.inputField = inputField;
    this.fieldMappings = new ArrayList<FieldMapping>( );
    for (FieldMapping fieldMapping : fieldMappings)
    {
     this.fieldMappings.add( fieldMapping );
    }
      
    this.parentIDFieldName = parentIDFieldName;
    this.dataObjectTransform = dataObjectTransform;
  }

  @JsonProperty( "format" )
  public Format getFormat( ) {
    return this.format;
  }
    
  @JsonProperty( "inputField" )
  public String getInputField( ) {
    return this.inputField;
  }
    
  @JsonProperty( "fieldMappings" )
  public List<FieldMapping> getFieldMappings(  ) {
    return this.fieldMappings;
  }
    
  @JsonProperty( "parentIDFieldName" )
  public String getParentIDFieldName( ) {
    return this.parentIDFieldName;
  }
    
  @JsonProperty( "dataObjectTransform" )
  public String getDataObjectTransform(  ) {
    return this.dataObjectTransform;
  }

  public static class FieldMapping {
    @SchemaProperty(title = "Input Path", required = true)
    public String inputPath;
      
    @SchemaProperty(title = "Solr Field", required = true)
    public String solrField;
      
    // mode is an enum:  Field Value, Linked Object, Sub Object, JSON String representation
    @SchemaProperty(title = "Mapping Mode", required = true )
    public Mode mode;
      
    @SchemaProperty(title = "Parent ID Field" )
    public String parentIDField = "parent_id_s";
      
    @SchemaProperty(title="Nested Object Fields" )
    public List<InnerMapping> innerMappings;
      
    @SchemaProperty(title="Copy Parent Fields" )
    public List<String> copyParentFields;
      
    @JsonCreator
    public FieldMapping( @JsonProperty("inputPath") String inputPath,
                         @JsonProperty("solrField") String solrField,
                         @JsonProperty("mode") Mode mode,
                         @JsonProperty("parentIDField") String parentIDField,
                         @JsonProperty("copyParentFields" ) List<String> copyParentFields,
                         @JsonProperty("innerMappings") List<InnerMapping> innerMappings ) {
      this.inputPath = inputPath;
      this.solrField = solrField;
      this.mode = mode;
      this.parentIDField = parentIDField;
      this.innerMappings = innerMappings;
        
      if (copyParentFields != null) {
        this.copyParentFields = new ArrayList<String>( copyParentFields );
      }
    }
  }
    
  public static class InnerMapping {
    @SchemaProperty(title = "Input Path", required = true)
    public String inputPath;
        
    @SchemaProperty(title = "Solr Field", required = true)
    public String solrField;
      
    @JsonCreator
    public InnerMapping( @JsonProperty("inputPath") String inputPath,
                         @JsonProperty("solrField") String solrField ) {
      this.inputPath = inputPath;
      this.solrField = solrField;
    }
  }
    
  public static enum Format {
      json, xml
  }
    
  public static enum Mode {
      field, linked_object, nested_object, json_string
  }
          
}
