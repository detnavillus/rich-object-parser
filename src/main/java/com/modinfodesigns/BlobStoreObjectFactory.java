package com.lucidworks.apollo.modinfodesigns;


import com.lucidworks.apollo.component.ResourceLoader;

import com.modinfodesigns.app.ModInfoObjectFactory;
import com.modinfodesigns.utils.DOMMethods;

import java.util.List;

import org.w3c.dom.Document;

import java.io.InputStream;
import java.io.IOException;

public class BlobStoreObjectFactory extends ModInfoObjectFactory {
    
  private ResourceLoader resourceLoader;
    

  public BlobStoreObjectFactory( ResourceLoader resourceLoader ) {
    this.resourceLoader = resourceLoader;
  }
    
  public BlobStoreObjectFactory( String configData ) {
    initialize( configData );
  }

  public void initialize( String configData ) {
      
    try {
      // get the InputStream from resourceLoader
      InputStream configStream = resourceLoader.get( configData );
      Document doc = DOMMethods.getDocument( configStream );
      super.initialize( doc.getDocumentElement( ) );
    }
    catch ( IOException ioe ) {
          
    }
  }
}