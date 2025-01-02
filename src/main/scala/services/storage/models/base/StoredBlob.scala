package com.sneaksanddata.arcane.framework
package services.storage.models.base

/**
 * Blob object metadata.  
 */
case class StoredBlob(
 /**
  * Additional metadata attached to this object.  
  */
 metadata: Map[String, String] = Map(),

 /**
  * Content hashsum.  
  */
 contentHash: Option[String] = None,

 /**
  * Content encoding, for example utf-8.  
  */
 contentEncoding: Option[String] = None,

 /**
  * Content type, for example text/plain.  
  */
 contentType: Option[String] = None,

 /**
  * Content length in bytes.  
  */
 contentLength: Option[Long] = None,

 /**
  * Blob filename. May contain full path, depending on the actual storage.  
  */
 name: String,

 /**
  * Last modified timestamp.  
  */
 lastModified: Option[Long] = None,

 /**
  * Created on timestamp.  
  */
 createdOn: Option[Long])  
