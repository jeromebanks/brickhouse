package brickhouse.udf.collect;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@Description(name="group_first",
		value = "_FUNC_(x) - Returns first element in the aggregation group"
)
public class FirstOfGroupUDAF extends UDAF {

	public static class FirstLongEvaluator implements UDAFEvaluator {

		private Long output;

		public FirstLongEvaluator() {
			super();
			init();
		}

		@Override
		public void init() {
			output = null;
		}

		public boolean iterate(Long value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Long terminatePartial() {
			return output;
		}

		public boolean merge(Long value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Long terminate() {
			return output;
		}
	}

	public static class FirstStringEvaluator implements UDAFEvaluator {

		private String output;

		public FirstStringEvaluator() {
			super();
			init();
		}

		@Override
		public void init() {
			output = null;
		}

		public boolean iterate(String value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public String terminatePartial() {
			return output;
		}

		public boolean merge(String value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public String terminate() {
			return output;
		}
	}

	public static class FirstFloatEvaluator implements UDAFEvaluator {

		private Float output;

		public FirstFloatEvaluator() {
			super();
			init();
		}

		@Override
		public void init() {
			output = null;
		}

		public boolean iterate(Float value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Float terminatePartial() {
			return output;
		}

		public boolean merge(Float value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Float terminate() {
			return output;
		}
	}

	public static class FirstDoubleEvaluator implements UDAFEvaluator {

		private Double output;

		public FirstDoubleEvaluator() {
			super();
			init();
		}

		@Override
		public void init() {
			output = null;
		}

		public boolean iterate(Double value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Double terminatePartial() {
			return output;
		}

		public boolean merge(Double value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Double terminate() {
			return output;
		}
	}

	public static class FirstIntEvaluator implements UDAFEvaluator {

		private Integer output;

		public FirstIntEvaluator() {
			super();
			init();
		}

		@Override
		public void init() {
			output = null;
		}

		public boolean iterate(Integer value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Integer terminatePartial() {
			return output;
		}

		public boolean merge(Integer value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Integer terminate() {
			return output;
		}
	}

	public static class FirstBooleanEvaluator implements UDAFEvaluator {

		private Boolean output;

		public FirstBooleanEvaluator() {
			super();
			init();
		}

		@Override
		public void init() {
			output = null;
		}

		public boolean iterate(Boolean value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Boolean terminatePartial() {
			return output;
		}

		public boolean merge(Boolean value) {
			if (value != null && output == null) {
				output = value;
			}
			return true;
		}

		public Boolean terminate() {
			return output;
		}
	}
}