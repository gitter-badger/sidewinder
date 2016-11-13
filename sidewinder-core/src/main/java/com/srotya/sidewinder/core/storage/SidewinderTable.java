/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core.storage;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @author Ambud
 */
public class SidewinderTable extends AbstractTable implements ScannableTable {

	private RelDataType types;
	private AbstractStorageEngine engine;
	private String seriesName;

	public SidewinderTable(String seriesName, AbstractStorageEngine engine) {
		this.seriesName = seriesName;
		this.engine = engine;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		types = typeFactory.builder().add("timestamp", SqlTypeName.TIMESTAMP).add("value", SqlTypeName.DOUBLE)
				.add("tags", SqlTypeName.MAP).build();
		return types;
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root) {
		return new AbstractEnumerable<Object[]>() {
			@Override
			public Enumerator<Object[]> enumerator() {
				return null;
			}
		};
		// public Enumerator<Object[]> enumerator() {
		// return new CsvEnumerator<>(file, cancelFlag, false,
		// null, new CsvEnumerator.ArrayRowConverter(fieldTypes, fields));
		// }
		// };
		// }
	}

}
