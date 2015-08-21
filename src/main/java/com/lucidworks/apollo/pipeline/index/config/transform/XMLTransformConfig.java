package com.lucidworks.apollo.pipeline.index.config.transform;

import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucidworks.apollo.pipeline.StageConfig;
import com.lucidworks.apollo.pipeline.schema.Annotations.Schema;
import com.lucidworks.apollo.pipeline.schema.Annotations.SchemaProperty;

@JsonTypeName("xml-transform")
@Schema(
 type = "xml-transform",
 title = "XML Transform Stage",
 description = "This stage transforms XML contained in body field to pipeline document fields"
)
public class XMLTransformConfig extends StageConfig {
  private transient static final Logger LOG = LoggerFactory.getLogger( XMLTransformConfig.class );

  public static final String KEEP_PARENT = "keepParent";
  public static final String METADATA = "metadata";
  public static final String MAPPINGS = "mappings";
  public static final String BODY_FIELD = "bodyField";
  public static final String DEFAULT_BODY_FIELD = "body";
  public static final String PARENT_ID_FIELD = "parentIdField";
  public static final String ROOT_XPATH = "rootXPath";
  public static final String SUB_DOCUMENT_FIELD = "subDocField";
  public static final String CLEANUP_XML = "cleanUpXML";

  @SchemaProperty(title = "Root XPath", name = ROOT_XPATH, required = true, defaultValue = "")
  private final String rootXPath;

  @SchemaProperty(title = "Parent ID Field Name", name = PARENT_ID_FIELD)
  private final String parentIdField;

  // field that has source XML
  @SchemaProperty(title = "Body Field Name", name = BODY_FIELD, defaultValue = DEFAULT_BODY_FIELD)
  private final String bodyField;

  @SchemaProperty(title = "XPath Mappings", name = MAPPINGS)
  private final List<XPathMappingRule> mappings;

  @SchemaProperty(title = "Additional Metadata", name = METADATA)
  private final List<AdditionalMetadata> metadata;

  @SchemaProperty(title = "Keep parent document", name = KEEP_PARENT, defaultValue = "false")
  private boolean keepParent;
    
  // Add an option to Create Sub Documents (needs a field name for this)
  // sub Document field - optional - if not null, add sub documents to this parent field)
  // if this is not null and keep parent is false - what does this mean??
  @SchemaProperty(title = "Sub Document Field", name=SUB_DOCUMENT_FIELD )
  private final String subDocField;
    
  @SchemaProperty(title="Cleanup XML Data", name=CLEANUP_XML, defaultValue="true")
  private final boolean cleanUpXML;
	
  /**
   * Creates configuration for the XMLTranformStage
   * @param id Configuration ID
   * @param rootXPath XPath-style path to the document's element to create new documents from
   * @param parentIdField Parent ID
   * @param bodyField Name of the field containing document's body
   * @param mappings Field names mapping
   * @param metadata Additional metadata
   */
  @JsonCreator
  public XMLTransformConfig(
      @JsonProperty("id") String id,
      @JsonProperty(ROOT_XPATH) String rootXPath,
      @JsonProperty(PARENT_ID_FIELD) String parentIdField,
      @JsonProperty(BODY_FIELD) String bodyField,
      @JsonProperty(MAPPINGS) List<XPathMappingRule> mappings,
      @JsonProperty(METADATA) List<AdditionalMetadata> metadata,
      @JsonProperty(KEEP_PARENT) Boolean keepParent,
      @JsonProperty(SUB_DOCUMENT_FIELD) String subDocField,
      @JsonProperty(CLEANUP_XML) Boolean cleanUpXML) {
    super(id);
    this.rootXPath = rootXPath;
    this.parentIdField = parentIdField;
    
    if(bodyField != null && bodyField.length() > 0) {
      this.bodyField = bodyField;
    } else {
      this.bodyField = DEFAULT_BODY_FIELD;
    }
    
    this.keepParent = keepParent == null ? false : keepParent;
      
    this.subDocField = subDocField;
    if (subDocField != null) this.keepParent = true;
      
    this.cleanUpXML = cleanUpXML == null ? false : cleanUpXML;
      
    if(mappings != null) {
      this.mappings = Collections.unmodifiableList(mappings);
    } else {
      this.mappings = Collections.emptyList();
    }
      
    if (metadata != null) {
      this.metadata = Collections.unmodifiableList( metadata );
    } else {
      this.metadata = Collections.emptyList();
    }
  }
    
  @JsonProperty(ROOT_XPATH)
  public String getRootXPath( ) {
    return this.rootXPath;
  }
    
  @JsonProperty(PARENT_ID_FIELD)
  public String getParentIdField( ) {
    return this.parentIdField;
  }
  
  @JsonProperty(MAPPINGS)
  public List<XPathMappingRule> getMappings() {
    return mappings;
  }
    
  @JsonProperty(METADATA)
  public List<AdditionalMetadata> getMetadata() {
    return metadata;
  }

  @JsonProperty(BODY_FIELD)
  public String getBodyField( ) {
    return this.bodyField;
  }
  
  @JsonProperty(KEEP_PARENT)
  public boolean isKeepParent() {
    return this.keepParent;
  }
    
  @JsonProperty(SUB_DOCUMENT_FIELD)
  public String getSubDocumentField( ) {
    return this.subDocField;
  }
    
  @JsonProperty(CLEANUP_XML)
  public boolean shouldCleanupXML( ) {
    return this.cleanUpXML;
  }
    
  public static class XPathMappingRule {

    public static final String XPATH = "xpath";
    public static final String FIELD = "field";
    public static final String MULTIVALUE = "multivalue";
    public static final String SAVE_AS_XML = "saveAsXML";
    public static final String FIELD_SUFFIX = "fieldSuffix";
    
    @SchemaProperty(title = "XPath Expression", name = XPATH, required = true)
    private final String xpath;
      
    // Can be an XPath or a field name - if XPath will start with a '/' character
    @SchemaProperty(title = "Field (name or path)", name = FIELD, required = true)
    private final String field;
      
    @SchemaProperty(title = "Field Name Suffix", name = FIELD_SUFFIX )
    private final String fieldSuffix;
      
    @SchemaProperty(title = "Multi Value", name = MULTIVALUE, defaultValue = "false")
    private final boolean multiValue;
      
    // Save Value As XML
    @SchemaProperty(title = "Save As XML", name=SAVE_AS_XML, defaultValue = "false" )
    private final boolean saveAsXML;
      
    // field name suffix
    
      
    
    /**
     * Creates mapping rule for the XML document's element
     * @param xpath XPath-style path to the XML document's element
     * @param field Name of the field mapped
     * @param multiValue Multivalue field flag
     */
    @JsonCreator
    public XPathMappingRule(
        @JsonProperty(XPATH) String xpath,
        @JsonProperty(FIELD) String field,
        @JsonProperty(FIELD_SUFFIX) String fieldSuffix,
        @JsonProperty(MULTIVALUE) Boolean multiValue,
        @JsonProperty(SAVE_AS_XML) Boolean saveAsXML ) {
      this.xpath = xpath;
      this.field = field;
      this.multiValue = multiValue == null ? false : multiValue;
        
      this.saveAsXML = saveAsXML == null ? false : saveAsXML;
        
        this.fieldSuffix = fieldSuffix != null ? fieldSuffix : "";
        
      LOG.info( "Field " + field + " multivalue = " + multiValue );
    }
        
    @JsonProperty(XPATH)
    public String getXpath() {
      return xpath;
    }
        
    @JsonProperty(FIELD)
    public String getField() {
      return field;
    }
      
    @JsonProperty(FIELD_SUFFIX)
    public String getFieldSuffix() {
      return fieldSuffix;
    }
      
    @JsonProperty(MULTIVALUE)
    public boolean getMultiValue( ) {
      return multiValue;
    }
    
    @JsonProperty(SAVE_AS_XML)
    public boolean getSaveAsXML( ) {
      return saveAsXML;
    }
      
    @Override
    public String toString() {
      return "XPathMappingRule [xpath=" + xpath + ", field=" + field + "]";
    }
  }
    
  public static class AdditionalMetadata {

    public static final String FIELD = "field";
    public static final String VALUE = "value";

    @SchemaProperty(title = "Field", name = FIELD, required = true )
    private final String field;
        
    @SchemaProperty(title = "Value", name = VALUE, required = true )
    private final String value;
    

    /**
     * Creates additional metadata
     * @param field Metadata field name
     * @param value Metadata field value
     */
    @JsonCreator
    public AdditionalMetadata(
        @JsonProperty(FIELD) String field,
        @JsonProperty(VALUE) String value) {
      this.field = field;
      this.value = value;
//      LOG.info( "Field " + field + " value = " + value );
    }

    @JsonProperty(FIELD)
    public String getField() {
      return field;
    }
        
    @JsonProperty(VALUE)
    public String getValue( ) {
      return value;
    }
  }
}
