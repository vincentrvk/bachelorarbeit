<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" attributeFormDefault="unqualified">
	<xs:element name="EmployeeBenefitsVendorData_List">
		<xs:complexType>
			<xs:sequence>
				<xs:element name="EmployeeBenefitsVendorData" minOccurs="0" maxOccurs="unbounded">
					<xs:complexType>
						<xs:sequence>
							<xs:element name="EmployeeID" type="xs:int"/>
							<xs:element name="EmployeeBenefitData">
								<xs:complexType>
									<xs:sequence>
										<xs:element name="ValidityPeriod">
											<xs:complexType>
												<xs:sequence>
													<xs:element name="StartDate" type="xs:date"/>
													<xs:element name="EndDate" type="xs:date"/>
												</xs:sequence>
											</xs:complexType>
										</xs:element>
										<xs:element name="CompensationComponentTypeID" type="xs:string"/>
										<xs:element name="Amount">
											<xs:complexType>
												<xs:attribute name="currencyCode" type="xs:string"/>
											</xs:complexType>
										</xs:element>
										<xs:element name="GoalAmount">
											<xs:complexType>
												<xs:attribute name="currencyCode" type="xs:string"/>
											</xs:complexType>
										</xs:element>
										<xs:element name="PercentageContribution" type="xs:int"/>
									</xs:sequence>
								</xs:complexType>
							</xs:element>
						</xs:sequence>
					</xs:complexType>
				</xs:element>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
</xs:schema>