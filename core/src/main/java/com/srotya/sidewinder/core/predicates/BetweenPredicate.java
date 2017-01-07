package com.srotya.sidewinder.core.predicates;

import java.util.Date;

public class BetweenPredicate implements Predicate {

	private long startTs;
	private long endTs;

	public BetweenPredicate(long startValue, long endValue) {
		this.startTs = startValue;
		this.endTs = endValue;
	}

	@Override
	public boolean apply(long value) {
		boolean result = value >= startTs && value <= endTs;
		if(!result) {
			System.out.println("Rejected"+new Date(value)+"\t"+new Date(startTs)+"\t"+new Date(endTs));
		}
		return result;
	}

}
