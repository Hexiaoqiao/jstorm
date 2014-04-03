package backtype.storm;

import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should
 * run your program with the "storm jar" command from the command-line, and then
 * use this class to submit your topologies.
 */
public class StormSubmitter {
	public static Logger LOG = LoggerFactory.getLogger(StormSubmitter.class);

	private static Nimbus.Iface localNimbus = null;

	public static void setLocalNimbus(Nimbus.Iface localNimbusHandler) {
		StormSubmitter.localNimbus = localNimbusHandler;
	}

	/**
	 * Submits a topology to run on the cluster. A topology runs forever or
	 * until explicitly killed.
	 * 
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	public static void submitTopology(String name, Map stormConf,
			StormTopology topology) throws AlreadyAliveException,
			InvalidTopologyException, TopologyAssignException {
		submitTopology(name, stormConf, topology, null);
	}

	/**
	 * Submits a topology to run on the cluster. A topology runs forever or
	 * until explicitly killed.
	 * 
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @param options
	 *            to manipulate the starting of the topology
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	public static void submitTopology(String name, Map stormConf,
			StormTopology topology, SubmitOptions opts)
			throws AlreadyAliveException, InvalidTopologyException,
			TopologyAssignException {
		if (!Utils.isValidConf(stormConf)) {
			throw new IllegalArgumentException(
					"Storm conf is not valid. Must be json-serializable");
		}
		stormConf = new HashMap(stormConf);
		stormConf.putAll(Utils.readCommandLineOpts());
		Map conf = Utils.readStormConfig();
		conf.putAll(stormConf);
		putUserInfo(conf, stormConf);
		try {
			String serConf = JSONValue.toJSONString(stormConf);
			if (localNimbus != null) {
				LOG.info("Submitting topology " + name + " in local mode");
				localNimbus.submitTopology(name, null, serConf, topology);
			} else {
				NimbusClient client = NimbusClient.getConfiguredClient(conf);
				if (topologyNameExists(conf, name)) {
					throw new RuntimeException("Topology with name `" + name
							+ "` already exists on cluster");
				}
				if (checkResource(topology, conf, client)) {
					throw new RuntimeException("Topology with name `" + name
							+ "` submit abort because lacking resource, please reduce" 
							+ "resource requirement and retry!");
				}
				submitJar(conf);
				try {
					LOG.info("Submitting topology " + name
							+ " in distributed mode with conf " + serConf);
					if (opts != null) {
						client.getClient().submitTopologyWithOpts(name,
								submittedJar, serConf, topology, opts);
					} else {
						// this is for backwards compatibility
						client.getClient().submitTopology(name, submittedJar,
								serConf, topology);
					}
				} catch (InvalidTopologyException e) {
					LOG.warn("Topology submission exception", e);
					throw e;
				} catch (AlreadyAliveException e) {
					LOG.warn("Topology already alive exception", e);
					throw e;
				} catch (TopologyAssignException e) {
					LOG.warn("Failed to assign ", e.get_msg());
					throw e;
				} finally {
					client.close();
				}
			}
			LOG.info("Finished submitting topology: " + name);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean topologyNameExists(Map conf, String name) {
		NimbusClient client = NimbusClient.getConfiguredClient(conf);
		try {
			ClusterSummary summary = client.getClient().getClusterInfo();
			for (TopologySummary s : summary.get_topologies()) {
				if (s.get_name().equals(name)) {
					return true;
				}
			}
			return false;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			client.close();
		}
	}

	// check whether resources of the cluster to reach the requirements  
	private static boolean checkResource(
			StormTopology topology, Map conf, 
			NimbusClient client) throws TException {
		ClusterSummary clusterSummary = client.getClient().getClusterInfo();
		List<SupervisorSummary> supervisors = clusterSummary.get_supervisors();
		int nFreeWorkers = 0;
		int nFreeCPU = 0;
		int nFreeMem = 0;
		int nFreeDisk = 0;
		if (supervisors == null || supervisors.size() == 0) {
			LOG.info("Cluster has no any alive supervisors!");
			return false;
		}
		for (SupervisorSummary supervisor : supervisors) {
			nFreeWorkers = nFreeWorkers 
				+ supervisor.get_num_workers() 
				- supervisor.get_num_used_workers();
			nFreeCPU = nFreeCPU
				+ supervisor.get_num_cpu()
				- supervisor.get_num_used_cpu();
			nFreeMem = nFreeMem
				+ supervisor.get_num_mem()
				- supervisor.get_num_used_mem();
			nFreeDisk = nFreeDisk
				+ supervisor.get_num_disk()
				- supervisor.get_num_used_disk();
		}
		int numTasks = 0;
		Map<String, SpoutSpec> spoutSpecs = topology.get_spouts();
		Map<String, Bolt> bolts = topology.get_bolts();
		Iterator iter = spoutSpecs.entrySet().iterator(); 
		while (iter.hasNext()) {
		    Map.Entry entry = (Map.Entry) iter.next();
		    SpoutSpec val = (SpoutSpec) entry.getValue();
		    int parallelhint = val.get_common().is_set_parallelism_hint() ? 
		    		val.get_common().get_parallelism_hint() : 1;
			Map<Object, Object> serializedConf = (Map<Object, Object>) JStormUtils.from_json(
					val.get_common().get_json_conf());
		    int tasks = 0;
		    Integer nt = (Integer) serializedConf.get(Config.TOPOLOGY_TASKS);
		    if (nt == null) {
		    	tasks = parallelhint;
		    } else if (nt.intValue() > parallelhint) {
		    	tasks = nt.intValue();
		    } else {
		    	tasks = parallelhint;
		    }
		    if (tasks <= 0) {
		    	tasks = 1;
		    }
		    numTasks = numTasks + tasks;
		} 
		int nNeedWorkers = (Integer) conf.get(Config.TOPOLOGY_WORKERS);
		int nNeedCPU = numTasks * ConfigExtension.getCpuSlotsPerTask(conf);
		int nNeedMem = numTasks * ConfigExtension.getMemSlotPerTask(conf);
		
		if (nFreeWorkers < nNeedWorkers) {
			LOG.info("Topology Submit Abort : No enough Net resource!");
			return false;
		}
		if (nFreeCPU < nNeedCPU) {
			LOG.info("Topology Submit Abort : No enough CPU resource!");
			return false;
		}
		if (nFreeMem < nNeedMem) {
			LOG.info("Topology Submit Abort : No enough Mem resource!");
			return false;
		}
		return true;
	}
	
	private static String submittedJar = null;

	private static void submitJar(Map conf) {
		if (submittedJar == null) {
			LOG.info("Jar not uploaded to master yet. Submitting jar...");
			String localJar = System.getProperty("storm.jar");
			submittedJar = submitJar(conf, localJar);
		} else {
			LOG.info("Jar already uploaded to master. Not submitting jar.");
		}
	}

	public static String submitJar(Map conf, String localJar) {
		if (localJar == null) {
			throw new RuntimeException(
					"Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
		}
		NimbusClient client = NimbusClient.getConfiguredClient(conf);
		try {
			String uploadLocation = client.getClient().beginFileUpload();
			LOG.info("Uploading topology jar " + localJar
					+ " to assigned location: " + uploadLocation);
			BufferFileInputStream is = new BufferFileInputStream(localJar);
			while (true) {
				byte[] toSubmit = is.read();
				if (toSubmit.length == 0)
					break;
				client.getClient().uploadChunk(uploadLocation,
						ByteBuffer.wrap(toSubmit));
			}
			client.getClient().finishFileUpload(uploadLocation);
			LOG.info("Successfully uploaded topology jar to assigned location: "
					+ uploadLocation);
			return uploadLocation;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			client.close();
		}
	}
	
	private static void putUserInfo(Map conf, Map stormConf) {
		stormConf.put("user.group", conf.get("user.group"));
		stormConf.put("user.name", conf.get("user.name"));
		stormConf.put("user.password", conf.get("user.password"));
	}
}
