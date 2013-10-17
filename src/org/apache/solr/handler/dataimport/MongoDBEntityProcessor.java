package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class <code>MongoDBEntityProcessor</code>: MongoDBEntityProcessor
 * <p>
 * $URL$
 * 
 * @author ZhengShanHui
 * @since 2013-7-5
 * @version $Rev$$Date$
 */
public class MongoDBEntityProcessor extends EntityProcessorBase {
	private static final Logger logger = LoggerFactory.getLogger(MongoDBEntityProcessor.class);
	private static final String COLLECTION = "collection";
	public static final String QUERY = "query";
	public static final String DELTA_QUERY = "deltaQuery";
	public static final String DELTA_IMPORT_QUERY = "deltaImportQuery";
	public static final String PARENT_DELTA_QUERY = "parentDeltaQuery";
	public static final String DEL_PK_QUERY = "deletedPkQuery";
	protected MongoDBDataSource mongoDBDataSource;
	private String collection;

	@Override
	public void init(Context context) {
		super.init(context);
		collection = context.getEntityAttribute(COLLECTION);
		if (collection == null) {
			throw new DataImportHandlerException(SEVERE, "collection is null");
		}
		mongoDBDataSource = (MongoDBDataSource) context.getDataSource();
	}

	protected void initQuery(String cmd) {
		try {
			DataImporter.QUERY_COUNT.get().incrementAndGet();
			rowIterator = mongoDBDataSource.getData(cmd, collection);
			this.query = cmd;
		} catch (DataImportHandlerException e) {
			throw e;
		} catch (Exception e) {
			logger.error("query failed: [" + query + "]", e);
			throw new DataImportHandlerException(SEVERE, e);
		}
	}

	@Override
	public Map<String, Object> nextRow() {
		if (rowIterator == null) {
			String cmd = getQuery();
			initQuery(cmd);
		}
		Map<String, Object> data = getNext();
		logger.debug("process: " + data);
		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.solr.handler.dataimport.EntityProcessorBase#nextModifiedRowKey()
	 */
	@Override
	public Map<String, Object> nextModifiedRowKey() {
		if (rowIterator == null) {
			String deltaQuery = context.getEntityAttribute(DELTA_QUERY);
			if (deltaQuery == null)
				return null;
			initQuery(context.replaceTokens(deltaQuery));
		}
		return getNext();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.solr.handler.dataimport.EntityProcessorBase#nextModifiedParentRowKey()
	 */
	@Override
	public Map<String, Object> nextModifiedParentRowKey() {
		if (rowIterator == null) {
			String parentDeltaQuery = context.getEntityAttribute(PARENT_DELTA_QUERY);
			if (parentDeltaQuery == null)
				return null;
			initQuery(context.replaceTokens(parentDeltaQuery));
		}
		return getNext();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.solr.handler.dataimport.EntityProcessorBase#nextDeletedRowKey()
	 */
	@Override
	public Map<String, Object> nextDeletedRowKey() {
		if (rowIterator == null) {
			String deletedPkQuery = context.getEntityAttribute(DEL_PK_QUERY);
			if (deletedPkQuery == null)
				return null;
			initQuery(context.replaceTokens(deletedPkQuery));
		}
		return getNext();
	}

	public String getQuery() {
		String query = context.getEntityAttribute(QUERY);
		if (Context.DELTA_DUMP.equals(context.currentProcess())) {
			String deltaCmd = context.getEntityAttribute(DELTA_IMPORT_QUERY);
			if (deltaCmd == null) {
				logger.warn("in delta mode, but delta command not find, use full command instead");
			} else {
				query = deltaCmd;
			}
		}
		query = context.replaceTokens(query);
		return query;
	}
}
