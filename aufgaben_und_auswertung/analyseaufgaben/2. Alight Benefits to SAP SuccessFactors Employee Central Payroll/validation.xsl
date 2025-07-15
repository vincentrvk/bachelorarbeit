<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" attributeFormDefault="unqualified">
       <xs:element name="RecordList">
              <xs:complexType>
                     <xs:sequence>
                            <xs:element name="Data" minOccurs="0" maxOccurs="unbounded">
                                   <xs:complexType>
                                          <xs:sequence>
                                                 <xs:element name="PersonID" type="xs:int"></xs:element>
                                                 <xs:element name="Validation" type="xs:string"></xs:element>
                                             </xs:sequence>
                                      </xs:complexType>
                               </xs:element>
                        </xs:sequence>
                 </xs:complexType>
          </xs:element>
   </xs:schema>