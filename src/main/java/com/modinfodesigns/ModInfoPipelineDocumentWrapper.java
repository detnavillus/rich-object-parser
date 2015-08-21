package com.lucidworks.apollo.modinfodesigns;

import com.lucidworks.apollo.common.pipeline.PipelineDocument;
import com.lucidworks.apollo.common.pipeline.PipelineField;

import com.modinfodesigns.property.DataObject;
import com.modinfodesigns.property.IProperty;
import com.modinfodesigns.property.PropertyList;

import com.modinfodesigns.property.string.StringProperty;
import com.modinfodesigns.property.quantity.IntegerProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

public class ModInfoPipelineDocumentWrapper extends DataObject {

  private PipelineDocument pipelineDoc;
    
  public ModInfoPipelineDocumentWrapper( PipelineDocument pipelineDoc ) {
    setPipelineDocument( pipelineDoc );
  }
    
  private void setPipelineDocument( PipelineDocument pipelineDoc ) {
    this.pipelineDoc = pipelineDoc;
      
    Map<String, List<PipelineField>> fieldMap = pipelineDoc.getFields();
    Map<String, IProperty> propMap =  convertMap( fieldMap );
      
    Iterator<IProperty> propIt = propMap.values().iterator( );
    while (propIt.hasNext( ))
    {
      super.addProperty( propIt.next( ) );
    }
  }
    
  public PipelineDocument getPipelineDocument( ) {
    return this.pipelineDoc;
  }
    

  @Override
  public String getID( ) {
    return pipelineDoc.getId();
  }

  @Override
  public void setID( String ID ) {
    pipelineDoc.setId( ID );
  }
    
  
  @Override
  public void addProperty( IProperty prop ) {
    super.addProperty( prop );
    addPipelineDocumentProperty( prop );
  }
    
  /**
   * Adds a property to the property holder - creates, appends multiple value properties
   */
  private void addPipelineDocumentProperty( IProperty property ) {
    String name = property.getName( );
    Object value = property.getValueObject( );
      
    pipelineDoc.addField( name, value );
  }

    
  @Override
  public void setProperty( IProperty prop ) {
    super.setProperty( prop );
    setPipelineDocumentProperty( prop );
  }

  private void setPipelineDocumentProperty( IProperty property ) {
    String name = property.getName( );
    Object value = property.getValueObject( );
      
    pipelineDoc.setField( name, value );
  }
    
  @Override
  public void removeProperty( String propName ) {
    super.removeProperty( propName );
    pipelineDoc.removeFields( propName );
  }
    
  @Override
  public void setProperties( Iterator<IProperty> properties ) {
    while (properties.hasNext( )){
      IProperty prop = properties.next( );
      setProperty( prop );
    }
  }

    
  private Map<String,IProperty> convertMap( Map<String, List<PipelineField>> fieldMap ) {
    HashMap<String,IProperty> propMap = new HashMap<String,IProperty>( );
    for (String name : fieldMap.keySet( )  ) {
      List<PipelineField> fields = fieldMap.get( name );
      propMap.put( name, getProperty( fields ));
    }
      
    return propMap;
  }
    
    
  private IProperty getProperty( List<PipelineField> fields ) {
    if (fields.size( ) > 1 ) {
      PropertyList pl = new PropertyList( );
      for (PipelineField field : fields ) {
        pl.addProperty( getProperty( field ) );
      }
      return pl;
    }
    else {
      return getProperty( fields.get( 0 ) );
    }
  }
    
  private IProperty getProperty( PipelineField field ) {
    String name = field.getName( );
    Object value = field.getValue( );
        
    // TODO check object type ...
    if (value instanceof Integer )
    {
      return new IntegerProperty( name, ((Integer)value).intValue( ) );
    }
        
        
    return new StringProperty( name, value.toString( ) );
  }

  
  @Override
  public IProperty copy( ) {
    PipelineDocument pdCopy = new PipelineDocument( this.pipelineDoc );
    return new ModInfoPipelineDocumentWrapper( pdCopy );
  }
  
}