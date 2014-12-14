/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.util;


import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.phoenix.schema.PDataType;

public class HiveTypeUtil {
	private static final Log LOG = LogFactory.getLog(HiveTypeUtil.class);

	private HiveTypeUtil() {
	}

	/**
	 * This method returns an array of most appropriates PDataType associated
	 * with a list of incoming hive types.
	 * 
	 * @param List
	 *            of TypeInfo
	 * @return Array PDataType
	 */
	public static PDataType[] hiveTypesToSqlTypes(List<TypeInfo> columnTypes)
			throws SerDeException {
		final PDataType[] result = new PDataType[columnTypes.size()];
		for (int i = 0; i < columnTypes.size(); i++) {
			result[i] = HiveType2PDataType(columnTypes.get(i));
		}
		return result;
	}

	/**
	 * This method returns the most appropriate PDataType associated with the
	 * incoming primitive hive type.
	 * 
	 * @param hiveType
	 * @return PDataType
	 */
	public static PDataType HiveType2PDataType(TypeInfo hiveType)
			throws SerDeException {
		System.out.println(" ConfigurstionUtil hiveTypeToPDataType");
		System.out.println(" typeInfo name " + hiveType.getTypeName());
		System.out.println("cat name " + hiveType.getCategory().name());
		switch (hiveType.getCategory()) {
		/* Integrate Complex types like Array */
		case PRIMITIVE:
			return HiveType2PDataType(hiveType.getTypeName());
		default:
			throw new SerDeException("Phoenix unsupported column type: "
					+ hiveType.getCategory().name());
		}
	}

	/**
	 * This method returns the most appropriate PDataType associated with the
	 * incoming hive type name.
	 * 
	 * @param hiveType
	 * @return PDataType
	 */
	public static PDataType HiveType2PDataType(String hiveType)
			throws SerDeException {
		System.out.println(" ConfigurstionUtil hiveTypeToPDataType");
		final String lctype = hiveType.toLowerCase();
		if ("string".equals(lctype)) {
			return PDataType.VARCHAR;
		} else if ("float".equals(lctype)) {
			return PDataType.FLOAT;
		} else if ("double".equals(lctype)) {
			return PDataType.DOUBLE;
		} else if ("boolean".equals(lctype)) {
			return PDataType.BOOLEAN;
		} else if ("tinyint".equals(lctype)) {
			return PDataType.SMALLINT;
		} else if ("smallint".equals(lctype)) {
			return PDataType.SMALLINT;
		} else if ("int".equals(lctype)) {
			return PDataType.INTEGER;
		} else if ("bigint".equals(lctype)) {
			return PDataType.DOUBLE;
		} else if ("timestamp".equals(lctype)) {
			return PDataType.TIMESTAMP;
		} else if ("binary".equals(lctype)) {
			return PDataType.BINARY;
		} else if ("date".equals(lctype)) {
			return PDataType.DATE;
		} else if ("array".equals(lctype)) {
			// return PArrayDataType
		}
		throw new SerDeException("Phoenix unrecognized column type: "
				+ hiveType);
	}
}