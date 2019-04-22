package com.hncy58.kafka.consumer.handler.extractor;

/**
 * 数据抽取器
 * @author	tokings
 * @company	hncy58	湖南长银五八
 * @website	http://www.hncy58.com
 * @version 1.0
 * @date	2019年4月22日 上午11:25:29
 *
 */
public interface Extractor<T> {

	public T extract(T src) throws Exception;
}
