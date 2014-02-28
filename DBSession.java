package dbconnect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DBSession {	
	private Connection connection = null;
	private Logger log = LoggerFactory.getLogger(DBSession.class);

	public DBSession(Connection connection) 
		throws SQLException, InterruptedException {
		log.debug("DBSession OPEN");		
		//this.connection = DB.getConnection("default");
		this.connection = connection;
		if (connection.getAutoCommit() == true)
			connection.setAutoCommit(false);
		if (connection == null) {
			log.error("OPEN: Connection is null");
			throw new SQLException("DBSession: Connection is null");
		}
	}

	public void close() {
		try {
			try {
				if (connection != null)
					connection.commit();
			} finally {
				connection.close();
			}

		} catch (Exception e) {
			log.error("CLOSE Exception: " + e.getMessage());
		}
		log.debug("DBSession CLOSE");
	}

	public Connection getConnection() {
		return connection;
	}

	public void commit() throws SQLException {
    	connection.commit();      
  	}
  
  	public void rollback() throws SQLException {
		connection.rollback();
  	}

  	public int executeBatch(String sql, Integer commitCount,
			ArrayList<Object[]> args) throws SQLException, Exception {
		String logprefix = "executeBatch";
		log.debug(logprefix + " SQL: {} BatchSize: {}", sql, args.size());
		int result = 0;
		int totalCount = 0;
		try {
			PreparedStatement p = null;
			try {
				p = connection.prepareStatement(sql);
				Iterator<Object[]> i = args.iterator();
				int batchCount = 0;
				while (i.hasNext()) {
					batchCount++;
					totalCount++;
					Object[] argArray = i.next();
					if (argArray != null)
						setParams(p, argArray);
					p.addBatch();
					if (totalCount % commitCount == 0) {
						log.debug(logprefix + " SQL: " + sql +
							" commit at " + totalCount);
						p.executeBatch();
						batchCount = 0;
						connection.commit();
					}
				}
				if (batchCount > 0) {
					log.debug(logprefix + " SQL: " + sql +
						" final commit at " + totalCount);
					p.executeBatch();
					connection.commit();
				}
				result = totalCount;
			} finally {
				if (p != null)
					p.close();
			}
		} catch (SQLException e) {
			log.error(logprefix + " SQLException: " + e.getMessage() +
				" SQL: " + sql + " args: " + args);
			throw new SQLException(e);
		} catch (Exception e) {
			log.error(logprefix + " Exception: " + e.getMessage() + " SQL: " +
					sql + " args: " + args);
			throw new Exception(e);
		}
		log.debug(logprefix + " SQL: {} result={}", sql, result);
		return result;
	}

	public int executeUpdate(String sql) throws SQLException, Exception {
		Object[] args = null;
		return executeUpdate(sql, args);
	}

	public int executeUpdate(String sql, Object arg0) throws SQLException,
			Exception {
		Object[] args = { arg0 };
		return executeUpdate(sql, args);
	}

	public int executeUpdate(String sql, Object arg0, Object arg1)
			throws SQLException, Exception {
		Object[] args = { arg0, arg1 };
		return executeUpdate(sql, args);
	}

	public int executeUpdate(String sql, Object arg0, Object arg1,
			Object arg2) throws SQLException, Exception {
		Object[] args = { arg0, arg1, arg2 };
		return executeUpdate(sql, args);
	}

	public int executeUpdate(String sql, Object[] args) throws SQLException,
			Exception {
		String logprefix = "executeUpdate";
		log.debug(logprefix + " SQL: {} Args: {}", sql, args);
		int result = 0;
		try {
			PreparedStatement p = null;
			try {
				p = connection.prepareStatement(sql);
				if (args != null)
					setParams(p, args);
				result = p.executeUpdate();
			} finally {
				if (p != null)
					p.close();
			}
		} catch (SQLException e) {
			log.error(logprefix + " SQLException: " + e.getMessage() +
				" SQL: " + sql + " args: " + args);
			throw new SQLException(e);
		} catch (Exception e) {
			log.error(logprefix + " Exception: " + e.getMessage() + " SQL: "
					+ sql + " args: " + args);
			throw new Exception(e);
		}
		log.debug(logprefix + " SQL: {} result={}", sql, result);
		return result;
	}

	public void setParams(PreparedStatement p, Object[] args)
			throws SQLException {
		for (int i = 0; i < args.length; i++) {
			p.setObject(i + 1, args[i]);
		}
	}

	public int execute(String sql) throws SQLException, Exception {
		Object[] args = null;
		return execute(sql, args);
	}

	public int execute(String sql, Object arg0) throws SQLException,
		Exception {
		Object[] args = { arg0 };
		return execute(sql, args);
	}

	public int execute(String sql, Object arg0, Object arg1)
			throws SQLException, Exception {
		Object[] args = { arg0, arg1 };
		return execute(sql, args);
	}

	public int execute(String sql, Object arg0, Object arg1, Object arg2)
			throws SQLException, Exception {
		Object[] args = { arg0, arg1, arg2 };
		return execute(sql, args);
	}

	public int execute(String sql, Object[] args) throws SQLException,
			Exception {
		String logprefix = "execute";
		log.debug(logprefix + " SQL: {} Args: {}", sql, args);
		int result = 0;
		try {
			PreparedStatement p = null;
			try {
				p = connection.prepareStatement(sql);
				if (args != null)
					setParams(p, args);
				boolean resultset = p.execute();
				if (!resultset)
					return p.getUpdateCount();
			} finally {
				if (p != null)
					p.close();
			}
		} catch (SQLException e) {
			log.error(logprefix + " SQLException: " + e.getMessage() +
				" SQL: " + sql + " args: " + args);
			throw new SQLException(e);
		} catch (Exception e) {
			log.error(logprefix + " Exception: " + e.getMessage() +
				" SQL: " + sql + " args: " + args);
			throw new Exception(e);
		}

		log.debug(logprefix + " SQL: {} result=", sql, result);

		return result;
	}

	public ArrayList<String[]> executeQuery(String sql, int maxcount)
			throws SQLException, Exception {
		Object[] args = null;
		return executeQuery(sql, maxcount, args);
	}

	public ArrayList<String[]> executeQuery(String sql, int maxcount,
			Object arg0) throws SQLException, Exception {
		Object[] args = { arg0 };
		return executeQuery(sql, maxcount, args);
	}

	public ArrayList<String[]> executeQuery(String sql, int maxcount,
			Object arg0, Object arg1) throws SQLException, Exception {
		Object[] args = { arg0, arg1 };
		return executeQuery(sql, maxcount, args);
	}

	public ArrayList<String[]> executeQuery(String sql, int maxcount,
			Object arg0, Object arg1, Object arg2) throws SQLException,
			Exception {
		Object[] args = { arg0, arg1, arg2 };
		return executeQuery(sql, maxcount, args);
	}

	public ArrayList<String[]> executeQuery(String sql, int maxcount,
			Object[] args) throws SQLException, Exception {
		String logprefix = "executeQuery";
		log.debug(logprefix + " SQL: {} Args: {}", sql, args);
		ArrayList<String[]> resultList = null;
		try {
			PreparedStatement p = null;
			try {
				p = connection.prepareStatement(sql);
				if (args != null)
					setParams(p, args);
				ResultSet rs = p.executeQuery();
				int mycount = 0;
				if (rs != null) {
					ResultSetMetaData rsmd = rs.getMetaData();
					int numberOfColumns = rsmd.getColumnCount();
					while (rs.next() && mycount < maxcount) {
						mycount++;
						if (resultList == null)
							resultList = new ArrayList<String[]>();
						String[] sArray;
						sArray = new String[numberOfColumns];
						for (int i = 1; i <= numberOfColumns; i++) {
							sArray[i - 1] = rs.getString(i);
						}
						resultList.add(sArray);
					}
				}
			} finally {
				if (p != null)
					p.close();
			}
		} catch (SQLException e) {
			log.error(logprefix + " SQLException: " + e.getMessage() 
				+ " SQL: " + sql + " args: " + args);
			throw new SQLException(e);
		} catch (Exception e) {
			log.error(logprefix + " Exception: " + e.getMessage() + " SQL: "
					+ sql + " args: " + args);
			throw new Exception(e);
		}
		if (resultList != null) {
			log.debug(logprefix + " SQL: {} resultList Size={}", sql,
					resultList.size());
		} else {
			log.debug(logprefix + " SQL: {} NO RESULTS", sql);
		}
		return resultList;
	}

}