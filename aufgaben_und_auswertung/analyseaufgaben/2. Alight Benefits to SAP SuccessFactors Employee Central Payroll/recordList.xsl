<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
	elementFormDefault="qualified" attributeFormDefault="unqualified">
	<xs:element name="RecordList">
		<xs:complexType>
			<xs:sequence>
				<xs:element name="Record" minOccurs="0" maxOccurs="unbounded">
					<xs:complexType>
						<xs:sequence>

							<xs:element name="Record_Type" type="xs:string"></xs:element>
							<xs:element name="Sequence_Number" type="xs:string"></xs:element>
							<xs:element name="ValidityPeriod_StartDate" type="xs:string"></xs:element>
							<xs:element name="ValidityPeriod_EndDate" type="xs:string"></xs:element>
							<xs:element name="PayrollID" type="xs:string"></xs:element>
							<xs:element name="CompenstationComponentTypeID" type="xs:string"></xs:element>
							<xs:element name="Amount" type="xs:string"></xs:element>
							<xs:element name="GoalAmount" type="xs:string"></xs:element>
							<xs:element name="PercentageContribution" type="xs:string"></xs:element>							
						</xs:sequence>
					</xs:complexType>
				</xs:element>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
</xs:schema>