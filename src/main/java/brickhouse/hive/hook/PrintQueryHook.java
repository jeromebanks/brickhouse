package brickhouse.hive.hook;

import java.util.Set;

import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

public class PrintQueryHook implements PreExecute {

	@Override
	public void run(SessionState session, Set<ReadEntity> reads,
			Set<WriteEntity> writes, UserGroupInformation ugi) throws Exception {
		HiveHistory history = session.getHiveHistory();
		
		session.out.println(" PreExecute Query " + session.getCmd());

	}

}
