/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.dynomitemanager.identity;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
//import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
//import com.netflix.astyanax.connectionpool.OperationResult;
//import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
//import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
//import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
//import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
//import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
//import com.netflix.astyanax.model.*;
//import com.netflix.astyanax.serializers.StringSerializer;
//import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import software.aws.mcs.auth.SigV4AuthProvider;

//import com.netflix.astyanax.AstyanaxContext;
//import com.netflix.astyanax.ColumnListMutation;
//import com.netflix.astyanax.Keyspace;
//import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.Host;
//import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
//import com.netflix.astyanax.connectionpool.OperationResult;
//import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
//import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
//import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
//import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
//import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
//import com.netflix.astyanax.model.*;
//import com.netflix.astyanax.serializers.StringSerializer;
//import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.TimeUUIDUtils;
import com.netflix.dynomitemanager.defaultimpl.IConfiguration;
import com.netflix.dynomitemanager.supplier.HostSupplier;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.now;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.toTimestamp;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;


@Singleton
public class InstanceDataDAOCassandra {
	private static final Logger logger = LoggerFactory.getLogger(InstanceDataDAOCassandra.class);

	private String CN_KEY = "key";
	private String CN_ID = "id";
	private String CN_APPID = "appid";
	private String CN_AZ = "availabilityzone";
	private String CN_DC = "datacenter";
	private String CN_INSTANCEID = "instanceid";
	private String CN_HOSTNAME = "hostname";
	private String CN_EIP = "elasticip";
	private String CN_TOKEN = "token";
	private String CN_LOCATION = "location";
	private String CN_VOLUME_PREFIX = "ssVolumes";
	private String CN_UPDATETIME = "updatetime";
	private String CN_CREATED = "created";
	private static final String CF_NAME_TOKENS = "tokens";
	private static final String CF_NAME_LOCKS = "locks";

	private final CqlSession bootSession;
	private final IConfiguration config;
	private final HostSupplier hostSupplier;
	private final String BOOT_CLUSTER;
	private final String KS_NAME;
	private final int cassandraPort;
	private long lastTimeCassandraPull = 0;
	private Set<AppsInstance> appInstances;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read  = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	@Inject
	public InstanceDataDAOCassandra(IConfiguration config, HostSupplier hostSupplier) {
		this.config = config;

		BOOT_CLUSTER = config.getCassandraClusterName();

		if (BOOT_CLUSTER == null || BOOT_CLUSTER.isEmpty())
			throw new RuntimeException(
					"Cassandra cluster name cannot be blank. Please use getCassandraClusterName() property.");

		KS_NAME = config.getCassandraKeyspaceName();

		if (KS_NAME == null || KS_NAME.isEmpty())
			throw new RuntimeException(
					"Cassandra Keyspace can not be blank. Please use getCassandraKeyspaceName() property.");

		cassandraPort = config.getCassandraPort();
		if (cassandraPort <= 0)
			throw new RuntimeException(
					"Port for Cassandra can not be blank. Please use getCassandratPort() property.");

		this.hostSupplier = hostSupplier;

		this.bootSession = init();
	}

	public void createInstanceEntry(AppsInstance instance) throws Exception {
		logger.info("*** Creating New Instance Entry ***");
		String key = getRowKey(instance);
		// If the key exists throw exception
		if (getInstance(instance.getApp(), instance.getRack(), instance.getId()) != null) {
			logger.info(String.format("Key already exists: %s", key));
			return;
		}

		getLock(instance);

		try {
			this.bootSession.execute(
					insertInto(CF_NAME_TOKENS)
							.value(CN_KEY, literal(key))
							.value(CN_ID, literal(instance.getId()))
							.value(CN_APPID, literal(instance.getApp()))
							.value(CN_AZ, literal(instance.getZone()))
							.value(CN_DC, literal(instance.getDatacenter()))
							.value(CN_LOCATION, literal(config.getRack()))
							.value(CN_INSTANCEID, literal(instance.getInstanceId()))
							.value(CN_HOSTNAME, literal(instance.getHostName()))
							.value(CN_EIP, literal(instance.getHostIP()))
							// 'token' is a reserved name in cassandra, so it needs to be double-quoted
							.value("\"" + CN_TOKEN + "\"", literal(instance.getToken()))
							//.value(CN_VOLUMES, literal(formatVolumes(instance.getVolumes())))
							.value(CN_UPDATETIME, now())
							.build()
							.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
			);
		} catch (final Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			releaseLock(instance);
		}
	}

	public void updateInstanceEntry(final AppsInstance instance) throws Exception {
		logger.info("*** Updating Instance Entry ***");
		String key = getRowKey(instance);
		if (getInstance(instance.getApp(), instance.getRack(), instance.getId()) == null) {
			logger.info(String.format("Key doesn't exist: %s", key));
			createInstanceEntry(instance);
			return;
		}

		getLock(instance);

		try {
			this.bootSession.execute(
					update(CF_NAME_TOKENS)
							.setColumn(CN_ID, literal(instance.getId()))
							.setColumn(CN_APPID, literal(instance.getApp()))
							.setColumn(CN_AZ, literal(instance.getZone()))
							.setColumn(CN_DC, literal(instance.getDatacenter()))
							.setColumn(CN_LOCATION, literal(config.getRack()))
							.setColumn(CN_INSTANCEID, literal(instance.getInstanceId()))
							.setColumn(CN_HOSTNAME, literal(instance.getHostName()))
							.setColumn(CN_EIP, literal(instance.getHostIP()))
							// 'token' is a reserved name in cassandra, so it needs to be double-quoted
							.setColumn("\"" + CN_TOKEN + "\"", literal(instance.getToken()))
							//.setColumn(CN_VOLUMES, literal(formatVolumes(instance.getVolumes())))
							.setColumn(CN_UPDATETIME, now())
							.whereColumn(CN_KEY).isEqualTo(literal(key))
							.build()
							.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
			);
		} catch (final Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			releaseLock(instance);
		}
	}

	private Map<String, String> formatVolumes(final Map<String, Object> volumes) {
		if (volumes == null) {
			return Collections.emptyMap();
		} else {
			Map<String, String> r = new HashMap<>();
			for (Map.Entry<String, Object> entry: volumes.entrySet()) {
				r.put(entry.getKey(), entry.getValue().toString());
			}
			return r;
		}
	}

	private List<Row> fetchRows(final String table, final String keyColumn, final String key) {
		return this.bootSession.execute(
				selectFrom(table)
						.all()
						.whereColumn(keyColumn).isEqualTo(literal(key))
						.build()
		).all();
	}

	private long rowCount(final String table, final String keyColumn, final String key) {
		List<Row> rows = this.bootSession.execute(
				selectFrom(table)
						.all()
						.whereColumn(keyColumn).isEqualTo(literal(key))
						.build()
		).all();
		return rows != null ? rows.size() : 0;
	}

	private void deleteExpired(AppsInstance instance, String key, int seconds) {
		Row row = this.bootSession.execute(
				"select toTimestamp(now()) from system.local"
		).one();
		Instant expiredTime = row.getInstant(0).minusSeconds(seconds);
		logger.info("deleting lock on key " + key + ", expired after " + expiredTime.toString());
		this.bootSession.execute(
				deleteFrom(CF_NAME_LOCKS)
						.whereColumn(CN_KEY).isEqualTo(literal(key))
						.whereColumn(CN_INSTANCEID).isEqualTo(literal(instance.getInstanceId()))
						.whereColumn(CN_CREATED).isLessThanOrEqualTo((literal(expiredTime.toString())))
						.build()
						.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
		);
	}

	/*
	 * To get a lock on the row - Create a choosing row and make sure there are
	 * no contenders. If there are bail out. Also delete the column when bailing
	 * out. - Once there are no contenders, grab the lock if it is not already
	 * taken.
	 */
	private void getLock(AppsInstance instance) throws Exception {
		final String choosingKey = getChoosingKey(instance);
		deleteExpired(instance, choosingKey, 6);
		Thread.sleep(100);
		this.bootSession.execute(
				insertInto(CF_NAME_LOCKS)
						.value(CN_KEY, literal(choosingKey))
						.value(CN_INSTANCEID, literal(instance.getInstanceId()))
						.value(CN_CREATED,toTimestamp(now()))
						.usingTtl(6)
						.build()
						.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
		);
		final long count = rowCount(CF_NAME_LOCKS, CN_KEY, choosingKey);
		if (count > 1) {
			// Need to delete my entry
			this.bootSession.execute(
					deleteFrom(CF_NAME_LOCKS)
							.whereColumn(CN_KEY).isEqualTo(literal(choosingKey))
							.whereColumn(CN_INSTANCEID).isEqualTo(literal(instance.getInstanceId()))
							.value(CN_CREATED,toTimestamp(now()))
							.build()
							.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
			);
			throw new Exception(String.format("More than 1 contender for lock %s %d", choosingKey, count));
		}

		final String lockKey = getLockingKey(instance);
		deleteExpired(instance, lockKey, 600);
		Thread.sleep(100);
		final List<Row> preCheck = fetchRows(CF_NAME_LOCKS, CN_KEY, lockKey);
		if (preCheck.size() > 0) {
			boolean found = false;
			for (Row row : preCheck) {
				if (instance.getInstanceId().equals(row.getString(CN_INSTANCEID))) {
					found = true;
					break;
				}
			}
			if (!found) {
				throw new Exception(String.format("Lock already taken %s", lockKey));
			}
		}

		this.bootSession.execute(
				insertInto(CF_NAME_LOCKS)
						.value(CN_KEY, literal(lockKey))
						.value(CN_INSTANCEID, literal(instance.getInstanceId()))
						.value(CN_CREATED,toTimestamp(now()))
						.build()
						.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
		);
		Thread.sleep(100);
		final List<Row> postCheck = fetchRows(CF_NAME_LOCKS, CN_KEY, lockKey);
		if (postCheck.size() == 1 && instance.getInstanceId().equals(postCheck.get(0).getString(CN_INSTANCEID))) {
			logger.info("Got lock " + lockKey);
		} else {
			throw new Exception(String.format("Cannot insert lock %s", lockKey));
		}
	}

	private void releaseLock(AppsInstance instance) throws Exception {
		final String choosingKey = getChoosingKey(instance);

		logger.info("releasing lock " + choosingKey);

		this.bootSession.execute(
				deleteFrom(CF_NAME_LOCKS)
						.whereColumn(CN_KEY).isEqualTo(literal(choosingKey))
						.whereColumn(CN_INSTANCEID).isEqualTo(literal(instance.getInstanceId()))
						.build()
						.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
		);
	}

	public void deleteInstanceEntry(AppsInstance instance) throws Exception {
		// Acquire the lock first
		getLock(instance);

		// Delete the row
		final String key = findKey(instance.getApp(), String.valueOf(instance.getId()), instance.getDatacenter(),
				instance.getRack());
		if (key == null)
			return; // don't fail it
		this.bootSession.execute(
				deleteFrom(CF_NAME_TOKENS)
						.whereColumn(CN_KEY).isEqualTo(literal(key))
						.build()
						.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
		);

		final String lockKey = getLockingKey(instance);
		this.bootSession.execute(
				deleteFrom(CF_NAME_LOCKS)
						.whereColumn(CN_KEY).isEqualTo(literal(lockKey))
						.whereColumn(CN_INSTANCEID).isEqualTo(literal(instance.getInstanceId()))
						.build()
						.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
		);

		final String choosingKey = getChoosingKey(instance);
		this.bootSession.execute(
				deleteFrom(CF_NAME_LOCKS)
						.whereColumn(CN_KEY).isEqualTo(literal(choosingKey))
						.whereColumn(CN_INSTANCEID).isEqualTo(literal(instance.getInstanceId()))
						.build()
						.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
		);
	}

	public AppsInstance getInstance(String app, String rack, String id) {
		Set<AppsInstance> set = getAllInstances(app);
		for (AppsInstance ins : set) {
			if (ins.getId().equals(id) && ins.getRack().equals(rack))
				return ins;
		}
		return null;
	}

	public Set<AppsInstance> getLocalDCInstances(String app, String region) {
		Set<AppsInstance> set = getAllInstances(app);
		Set<AppsInstance> returnSet = new HashSet<AppsInstance>();

		for (AppsInstance ins : set) {
			if (ins.getDatacenter().equals(region))
				returnSet.add(ins);
		}
		return returnSet;
	}

	private Set<AppsInstance> getAllInstancesFromCassandra(String app) {
		Set<AppsInstance> set = new HashSet<AppsInstance>();
		try {

			final List<Row> rows = this.bootSession.execute(
					selectFrom(CF_NAME_TOKENS)
							.all()
							.whereColumn(CN_APPID).isEqualTo(literal(app))
							.allowFiltering()
							.build()
			).all();
			for (final Row row : rows) {
				set.add(transform(row));
			}
		} catch (Exception e) {
			logger.warn("Caught an Unknown Exception during reading msgs ... -> " + e.getMessage());
			throw new RuntimeException(e);
		}
		return set;
	}

	public Set<AppsInstance> getAllInstances(String app) {
		write.lock();
		logger.debug("lastpull %d msecs ago, getting instances from C*", System.currentTimeMillis() - lastTimeCassandraPull);
		appInstances = getAllInstancesFromCassandra(app);
		lastTimeCassandraPull = System.currentTimeMillis();
		write.unlock();
		read.lock();
		Set<AppsInstance> retInstances = appInstances;
		read.unlock();
		return retInstances;
	}

	private String findKey(String app, String id, String location, String datacenter) {
		try {
			final Row row = this.bootSession.execute(
					selectFrom(CF_NAME_TOKENS)
							.all()
							.whereColumn(CN_APPID).isEqualTo(literal(app))
							.whereColumn(CN_ID).isEqualTo(literal(id))
							.whereColumn(CN_LOCATION).isEqualTo(literal(location))
							.whereColumn(CN_DC).isEqualTo(literal(datacenter))
							.allowFiltering()
							.build()
			).one();
			if (row == null) {
				return null;
			}

			return row.getString(CN_KEY);
		} catch (Exception e) {
			logger.warn("Caught an Unknown Exception during find a row matching cluster[" + app + "], id[" + id
					+ "], and region[" + datacenter + "]  ... -> " + e.getMessage());
			throw new RuntimeException(e);
		}

	}

	private AppsInstance transform(final Row row) {
		final AppsInstance ins = new AppsInstance();
		ins.setApp(row.getString(CN_APPID));
		ins.setZone(row.getString(CN_AZ));
		ins.setHost(row.getString(CN_HOSTNAME));
		ins.setHostIP(row.getString(CN_EIP));
		ins.setId(row.getString(CN_ID));
		ins.setInstanceId(row.getString(CN_INSTANCEID));
		ins.setDatacenter(row.getString(CN_DC));
		ins.setRack(row.getString(CN_LOCATION));
		ins.setToken(row.getString(CN_TOKEN));
		return ins;
	}

	private String getChoosingKey(AppsInstance instance) {
		return instance.getApp() + "_" + instance.getRack() + "_" + instance.getId() + "-choosing";
	}

	private String getLockingKey(AppsInstance instance) {
		return instance.getApp() + "_" + instance.getRack() + "_" + instance.getId() + "-lock";
	}

	private String getRowKey(AppsInstance instance) {
		return instance.getApp() + "_" + instance.getRack() + "_" + instance.getId();
	}

	private CqlSession init() {
		final String datacenter = config.getCassandraDatacenter();
		logger.info("cassandra datacenter: " + datacenter);
		logger.info("cassandra keyspace: " + KS_NAME);
		logger.info("cassandra port: " + cassandraPort);
		logger.info("Amazon Keyspaces enabled:" + String.valueOf(config.isAmazonKeyspacesSupplierEnabled()));

		if (config.isAmazonKeyspacesSupplierEnabled()) {
			List<InetSocketAddress> contactPoints =
					Collections.singletonList(
							InetSocketAddress.createUnresolved(config.getCassandraSeeds(), config.getCassandraPort()));
			try {
				return CqlSession.builder()
						.addContactPoints(contactPoints)
						.withSslContext(SSLContext.getDefault())
						.withAuthProvider(new SigV4AuthProvider(datacenter))
						.withLocalDatacenter(datacenter)
						.withKeyspace(KS_NAME)
						.build();
			}
			catch (NoSuchAlgorithmException e) {
				logger.error("Unsupprted algorithm: ", e.getMessage());
				throw new RuntimeException(e);
			}
		}
		final Supplier<List<Host>> supplier;
		if (config.isEurekaHostsSupplierEnabled()) {
			supplier = this.hostSupplier.getSupplier(BOOT_CLUSTER);
		} else {
			supplier = getSupplier();
		}

		final List<InetSocketAddress> contactPoints = new ArrayList<>();
		for (Host host : supplier.get()) {
			int port = host.getPort() > 0 ? host.getPort() : cassandraPort;
			contactPoints.add(new InetSocketAddress(host.getIpAddress(), port));
			logger.info("added contact point: " + host.getIpAddress());
		}

		return CqlSession.builder()
				.addContactPoints(contactPoints)
				.withLocalDatacenter(datacenter)
				.withKeyspace(KS_NAME)
				.build();
	}

	private Supplier<List<Host>> getSupplier() {

		return new Supplier<List<Host>>() {

			@Override
			public List<Host> get() {

				List<Host> hosts = new ArrayList<Host>();

				List<String> cassHostnames = new ArrayList<String>(
						Arrays.asList(StringUtils.split(config.getCassandraSeeds(), ",")));

				if (cassHostnames.size() == 0)
					throw new RuntimeException(
							"Cassandra Host Names can not be blank. At least one host is needed. Please use getCassandraSeeds() property.");

				for (String cassHost : cassHostnames) {
					logger.info("Adding Cassandra Host = {}", cassHost);
					hosts.add(new Host(cassHost, cassandraPort));
				}

				return hosts;
			}
		};
	}
}