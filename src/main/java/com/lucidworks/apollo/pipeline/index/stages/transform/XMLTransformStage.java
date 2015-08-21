package com.lucidworks.apollo.pipeline.index.stages.transform;

import java.io.StringReader;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucidworks.apollo.common.pipeline.PipelineDocument;
import com.lucidworks.apollo.common.pipeline.PipelineField;
import com.lucidworks.apollo.pipeline.PipelineCollector;
import com.lucidworks.apollo.pipeline.PipelineContext;
import com.lucidworks.apollo.pipeline.StageCallback;
import com.lucidworks.apollo.pipeline.index.IndexStage;
import com.lucidworks.apollo.pipeline.index.config.transform.XMLTransformConfig;
import com.lucidworks.apollo.pipeline.index.config.transform.XMLTransformConfig.AdditionalMetadata;
import com.lucidworks.apollo.pipeline.index.config.transform.XMLTransformConfig.XPathMappingRule;

import com.lucidworks.apollo.pipeline.stages.Stage;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.List;

@Stage
public class XMLTransformStage extends IndexStage<XMLTransformConfig> {
  private transient static final Logger Log = LoggerFactory.getLogger( XMLTransformStage.class );

  public static final String RESOURCE_SEPARATOR = "#";
    
  @Override
  public void process( PipelineDocument pipelineDoc, PipelineContext pipelineContext, XMLTransformConfig config,
                       PipelineCollector<PipelineDocument> collector, StageCallback<PipelineDocument> callback )
                                   throws Exception {
                                       
    Log.info( "process ..." );
    int doc_n = 0;
                                       
    String rootXPath = config.getRootXPath( );
    Log.info( "Using root XPath = " + rootXPath );
    XPath xPath =  XPathFactory.newInstance().newXPath();
    XPathExpression nodeExpr = xPath.compile( rootXPath );
                                       
    List<XPathMappingRule> xpathMappings = config.getMappings();
    List<AdditionalMetadata> additionalMetadata = config.getMetadata();
                                       
    boolean keepParentDoc = config.isKeepParent( );
    String subDocField = (keepParentDoc) ? config.getSubDocumentField( ) : null;
                                       
    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();

    List<PipelineField> xmlFields = pipelineDoc.getFields( config.getBodyField( ) );
    if ( xmlFields != null ) {
      for ( PipelineField xmlField : xmlFields ) {
        Object fieldVal = xmlField.getValue();
        
        // check if body is XML...
        String xmlStr = fieldVal.toString( ).trim( );
        if (xmlStr.trim().startsWith( "<?xml" ) || ( xmlStr.startsWith( "<" ) && xmlStr.endsWith( ">" )) ) {
          
          try {
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            Document xmlDocument = builder.parse( new ByteArrayInputStream( xmlStr.getBytes() ) );
            NodeList nodes = (NodeList)nodeExpr.evaluate( xmlDocument, XPathConstants.NODESET );
            Log.info( "Got " + nodes.getLength() + " nodes" );
              
            for (int i = 0; i < nodes.getLength(); i++) {
              Node node = nodes.item( i );
              Document subDoc = builder.parse( new ByteArrayInputStream( getText( node ).getBytes() ) );
                
              PipelineDocument pDoc = (!keepParentDoc || subDocField != null) ? new PipelineDocument( pipelineDoc ) : pipelineDoc;

              for ( XPathMappingRule xpathRule : xpathMappings ) {
                Log.info( "evaluating '" + xpathRule.getXpath( ) + "'" );
                XPathExpression valExpr = xPath.compile( xpathRule.getXpath( ) );
              
                String fieldname = xpathRule.getField( );
                if (fieldname != null && fieldname.startsWith( "/" )) {
                  XPathExpression fieldExpr = xPath.compile( fieldname );
                  // for each subDoc
                  NodeList fieldNodes = (NodeList)fieldExpr.evaluate( subDoc, XPathConstants.NODESET );
                  NodeList textNodes  = (NodeList)valExpr.evaluate( subDoc, XPathConstants.NODESET );
                  for (int n = 0; n < fieldNodes.getLength() && n < textNodes.getLength(); n++) {
                    Node fieldN = fieldNodes.item( n );
                    Node textN  = textNodes.item( n );
                    String fld = fieldN.getTextContent();
                    fld = fld.replace( " ", "_" );
                    fld = fld.replace( ":", "." );
                    fld = fld.replace( "/", "." );
                    fld = fld + xpathRule.getFieldSuffix( );
                      
                    pDoc.addField( fld, textN.getTextContent( ) );
                  }
                
                }
                else {
                  if ( xpathRule.getSaveAsXML( ) ) {
                    NodeList subnodes = (NodeList)valExpr.evaluate( subDoc, XPathConstants.NODESET );
                    for (int n = 0; n < subnodes.getLength(); n++) {
                      Node subnode = subnodes.item( n );
                      String value = getText( subnode );
                      Log.info( "addField: " + fieldname + " = '" + value + "'" );
                      pDoc.addField( fieldname, value );
                    }
                  }
                  else {
                    Log.info( "getting text: " + getText( node ));
                    List<String> values = getValues( subDoc, valExpr );
                    for (String val : values ) {
                      pDoc.addField( fieldname, val );
                    }
                  }
                }
              }
                
              if (additionalMetadata != null) {
                for (AdditionalMetadata fieldval : additionalMetadata ) {
                  pDoc.addField( fieldval.getField(), fieldval.getValue() );
                }
              }
                
              if (!keepParentDoc || subDocField != null) {
                String parentID = pipelineDoc.getId( );
                String recID = parentID + RESOURCE_SEPARATOR + Integer.toString( doc_n++ );
                pDoc.setId( recID );
                    
                if (config.getParentIdField() != null) {
                  pDoc.addField( config.getParentIdField( ), parentID );
                }
              }
              
              if ( !keepParentDoc ) {
                collector.write( pDoc );
              }
              else if ( subDocField != null ) {
                pipelineDoc.addField( subDocField, pDoc );
              }
            }
          }
          catch (ParserConfigurationException e) {
            throw new Exception( e.getMessage( ) );
          }
        }
      }
    }

    if ( keepParentDoc ) {
      if ( config.shouldCleanupXML( ) ) {
        pipelineDoc.removeFields( config.getBodyField() );
      }
      collector.write( pipelineDoc );
    }
  }

  @Override
  public Class<XMLTransformConfig> getStageConfigClass() {
    return XMLTransformConfig.class;
  }

    
  private String getText( Node n ) throws Exception {
    StringWriter writer = new StringWriter( );
    Transformer transformer = TransformerFactory.newInstance().newTransformer( );
    DOMSource source = new DOMSource( n );
    StreamResult result = new StreamResult( writer );
    transformer.transform( source, result );
    return writer.toString( );
  }
    
  private List<String> getValues( Document doc, XPathExpression valExpr ) {
    ArrayList<String> values = new ArrayList<String>( );
    try {
      NodeList textNodes = (NodeList)valExpr.evaluate( doc, XPathConstants.NODESET );
      for (int n = 0; n < textNodes.getLength(); ++n) {
        values.add( textNodes.item( n ).getTextContent() );
      }
      return values;
    }
    catch ( Exception e ) {
      Log.error( "getValue( ) Got Exception: " + e );
      return values;
    }
  }

}
