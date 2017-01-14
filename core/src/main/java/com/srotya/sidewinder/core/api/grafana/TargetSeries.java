package com.srotya.sidewinder.core.api.grafana;

import java.util.List;

public class TargetSeries {

	private String measurementName;
	private String fieldName;
	private List<String> filters;

	public TargetSeries(String measurementName, String fieldName, List<String> filters) {
		this.measurementName = measurementName;
		this.fieldName = fieldName;
		this.filters = filters;
	}

	/**
	 * @return the measurementName
	 */
	public String getMeasurementName() {
		return measurementName;
	}

	/**
	 * @param measurementName
	 *            the measurementName to set
	 */
	public void setMeasurementName(String measurementName) {
		this.measurementName = measurementName;
	}

	/**
	 * @return the fieldName
	 */
	public String getFieldName() {
		return fieldName;
	}

	/**
	 * @param fieldName
	 *            the fieldName to set
	 */
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	/**
	 * @return the filters
	 */
	public List<String> getFilters() {
		return filters;
	}

	/**
	 * @param filters
	 *            the filters to set
	 */
	public void setFilters(List<String> filters) {
		this.filters = filters;
	}
}