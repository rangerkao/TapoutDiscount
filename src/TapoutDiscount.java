import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TapoutDiscount {
	
	Logger logger = null;
	Properties props = new Properties();
	Connection conn = null;
	Statement st = null;
	ResultSet rs = null;
	
	String inputFile = null;
	String backFile = null;
	
	public static void main(String[] args) {
		args = new String[] {"CDHKGBMTWNLD02191.434186244"};
		if(args.length<1) {
			System.out.println("參數不足");
			return;
		}
		new TapoutDiscount(args[0]);
	}
	
	
	TapoutDiscount(String fileName){
		
		boolean loggerStatus = false;
		
		try {
			//載入設定檔
			loadProperties();
			//設定Log4J
			initialLog4j();
			//前置設定完畢
			loggerStatus = true;
		} catch (FileNotFoundException e1) {
			errorHandle(e1);
		} catch (IOException e1) {
			errorHandle(e1);
		}
		

		if(loggerStatus) {
			boolean isprocessed = false;
			try {
				//建立連線
				createConnection();
				//開始處理
				logger.info("process Start...");
				process(fileName);
				logger.info("process end...");

				isprocessed = true;
			} catch (SQLException e) {
				errorHandle(e);
			} catch (ClassNotFoundException e) {
				errorHandle(e);
			} catch (ParseException e) {
				errorHandle(e);
			} catch (IOException e) {
				errorHandle(e);
			} catch (Exception e) {
				errorHandle(e);
			}finally {
				if(!isprocessed) {
					try {
						logger.info("rollback");
						conn.rollback();
					} catch (SQLException e1) {}
				}
				closeConnection();
			}			
		}
		
		

	}

	
	
	public void process(String fileName) throws Exception {
		inputFile=fileName;
		backFile=fileName+".bak";
		//讀取檔案
		readFile();
		//取得CDR中用戶的優惠項目
		getDiscount();
		
		//TODO 除錯用，列出所撈到的優惠項目
		/*for(String imsi : userDiscount.keySet()) {
			for(Map<String,Object> discount : userDiscount.get(imsi)) {
				for(String keyName: discount.keySet()) {
					System.out.print(keyName+":"+discount.get(keyName)+",");
				}
				System.out.println();
			}
		}*/
		
		//處理CDR內容
		processCDR();
		
		//將CDR匯入Table
		insertCDR();
		
		//將使用者優惠更新
		updateUserDiscount();
		
		//轉存檔案
		saveFile();
		
		
	}
	
	public void getDiscount() throws SQLException {
		logger.info("getDiscount...");
		
		userDiscount.clear();
		//撈取所有用戶的優惠
		String imsis = "";
		int imsiCount = 0;
		//載入優惠資料
		for(String imsi : imsiSet) {
			if(!"".equals(imsis))
				imsis += ",";
			imsis += imsi;
			
			imsiCount++;
			
			if(imsiCount==1000) {
				queryDiscount(imsis);
				imsis = "";
				imsiCount = 0;
			}
		}
		
		if(!"".equals(imsis))
			queryDiscount(imsis);
		
		logger.info("end...");
	}
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	List<Map<String, Object>> newCDRList = new ArrayList<Map<String, Object>>();
	
	public void processCDR() throws ParseException {
		//進行CDR優惠折扣
		newCDRList.clear();
		for(Map<String,Object> cdr:cdrList) {
			Map<Integer,String> m = (Map<Integer, String>) cdr.get("data");
			//取得IMSI
			String imsi = m.get(145);
			//有CDR找不到IMSI，此檔案不處理作廢
			if(imsi == null ) {
				errorHandle("It has cdr Cann't find imsi.");
				break;
			}

			//取得CDR類型
			/**
			 * 0,78,79 voice
			 * 3 sms
			 * 53 data
			 */
			int type = Integer.parseInt(m.get(66));		
			
			//取得通話位置，目前使用在判斷是否為回國轉接
			//String callerArea = m.get(128);
			String calleeArea = m.get(129);
			
			//通話時間ms
			//12 start time, 13 end time
			long duration = 0;
			if(m.get(12)!=null && m.get(13)!=null)
				duration = sdf.parse(m.get(13)).getTime() - sdf.parse(m.get(12)).getTime();

			//檔案傳輸大小KB
			//36 Data Volume Outgoing, 37 Data Volume Incoming
			long volume = (m.get(36)==null?0:Long.parseLong(m.get(36)))+(m.get(37)==null?0:Long.parseLong(m.get(37)));
			
			//取得charge，以尋找累加的方式，防止有多個CDR tag出現
			
			Float charge = 0f;
			Integer firstChargeTag = null;

			for(int tag : m.keySet()) {
				//從40001開始成對出現，所以只看奇數Tag即可
				if(tag>40000 && tag%2==1) {
					
					charge += Float.parseFloat(m.get(tag));
					
					if(firstChargeTag == null)
						firstChargeTag = tag ;
					else {
						//如果出現2個Charge Tag，與之前預想狀況不一樣，丟出警示，加總後歸0
						errorHandle("more than one Charge Tag in cdr imsi = "+imsi+",tag = "+tag+".");
						m.put(tag, "0");
					}
				}
			}
			
			//記錄原始Charge
			Float oCharge = charge;
			//紀錄已使用的Item
			String discountItem = null;
			int discountUsage = 0;
			
			
			if(charge>0) {
				List<Map<String,Object>> discountList = (List<Map<String, Object>>) userDiscount.get(imsi);
				if(discountList!=null) {
					
					List<Map<String,Object>> newList = new ArrayList<Map<String,Object>>();
					discountItem = ""; 
					
					for(Map<String,Object> discount : discountList) {
						
						float left = (Float)discount.get("LEFT");
						float used = (Float)discount.get("USED");
						if(left>0) {
							String discountType = (String) discount.get("TYPE");
							
							
							//設定 00 為月租費減免35塊
							if("00".equals(discountType)) {
								
								if(!"".equals(discountItem))
									discountItem += ",";

								//判斷此筆CDR是否適用
								if(type == 0 || (type == 78 && !calleeArea.startsWith("#"))|| type == 79 ) {
									
									discountItem += discount.get("ID");
									
									float remain = 0;
									
									remain = charge - left;
									
									if(remain>=0) {
										used += left;
										left = 0;
										charge = remain;
									}else {
										used += left + remain;
										left = 0 - remain;
										charge = 0f;
									}
									
									discount.put("LEFT", left);
									discount.put("USED", used);
									//回寫新的Charge
									m.put(firstChargeTag, charge.toString());
									//因為Charge成對出現，所以也要修改後面的
									m.put(firstChargeTag+1, charge.toString());
								}
							}
						}
						newList.add(discount);
					}
					userDiscount.put(imsi, newList);
				}
			}
			//自定義額外欄位
			m.put(50000, firstChargeTag.toString()); //first charge tag
			m.put(50001, oCharge.toString()); //totalcharge
			m.put(50002, String.valueOf(oCharge-charge)); //discount
			m.put(50003, charge.toString()); //final charge
			m.put(50004, String.valueOf(discountUsage)); //DISCOUNT USAGE
			m.put(50005, discountItem); //DISCOUNT ITEM
			cdr.put("data", m);
			newCDRList.add(cdr);
		}
	}
	
	public void saveFile() throws Exception {
		
		File fi = new File(inputFile);
		File fb = new File(backFile);
		//將檔案改名
		if(!fi.renameTo(fb)) {
			throw new Exception("cannot create file bak.");
		}
		
		StringBuffer content = new StringBuffer();
		
		//將cdr轉換成輸出文字
		for(Map<String,Object> cdr:newCDRList) {
			content.append(outPutCDR(cdr));
			content.append("\n");
		}

		//輸出至檔案
	
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fi)));
			writer.append(content);

		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public void readFile() throws Exception {
		File fi = new File(inputFile);
		BufferedReader reader = null;
		String str = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(fi)));
			
			imsiSet.clear();
			cdrList.clear();
			while ((str = reader.readLine()) != null) {
				//將CDR讀出
				parseCDR(str);				
			}	
		}finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {}
			}
		}
	}
	
	List<Map<String,Object>> cdrList = new ArrayList<Map<String,Object>>();
	Set<String> imsiSet = new HashSet<String>();
	public void parseCDR(String cdr){
		int tag = 1;
		
		//分析CDR
		Map<Integer,String> m = new HashMap<Integer,String>();
		List<Integer> tags = new ArrayList<Integer>();
		
		
		for(String value : cdr.split(",")) {

			if(value.split("/").length>1) {
				tag = Integer.parseInt(value.split("/")[0]);
				value = value.split("/")[1];
			}
			
			m.put(tag, value);
			tags.add(tag);
			tag++;	
		}
		
		Map<String,Object> m2 = new HashMap<String,Object>();
		m2.put("tags", tags);
		m2.put("data", m);
		cdrList.add(m2);
		
		imsiSet.add(m.get(145));
		
		
	}
	
	public String outPutCDR(Map<String,Object> cdr) {
		//輸出的CDR
		int tag = 1;
		
		Map<Integer,String> m = (Map<Integer, String>) cdr.get("data");
		List<Integer> tags = (List<Integer>) cdr.get("tags");
		
		
		String output = "";
		for(int t : tags){
			
			if(!"".equals(output)) {
				output += ",";
			}
			
			if(tag!=t) {
				tag = t;
				output += tag+"/"+m.get(t);
			}else {
				output += m.get(t);
			}
			
			tag++ ;
		}
		
		
		return output;
	}
	
	
	
	//<imsi,List <DetailName,DetailValue>>>
	Map<String,List<Map<String,Object>>> userDiscount = new HashMap<String,List<Map<String,Object>>>();

	public void queryDiscount(String imsis) throws SQLException {
		
		String sql = "" +
				"select a.IMSI,a.SERVICEID,a.DISCOUNT_ID,a.used_amount,a.left_amount,b.DISCOUNT_TYPE,b.PRIORITY " + 
				"from (  select d.IMSI,c.SERVICEID,c.used_amount,c.left_amount,DISCOUNT_ID " + 
				"        from TAPOUT_DISCOUNT_USER_DISCOUNT c, IMSI d " + 
				"        where c.serviceid = d.serviceid " + 
				"        and sysdate between to_date(c.START_TIME,'yyyyMMddhh24miss')and to_date(c.END_TIME,'yyyyMMddhh24miss') " + 
				"        and d.imsi in ("+imsis+") )a , " + 
				"TAPOUT_DISCOUNT_ITEM b " + 
				"where a.DISCOUNT_ID = b.id order by b.PRIORITY ";
		
		rs = null;
		
		logger.info(sql);
		rs = st.executeQuery(sql);
		
		while(rs.next()) {
			
			String imsi = rs.getString("IMSI");
			
			List<Map<String,Object>> l = null;

			if(userDiscount.containsKey(imsi))
				l = userDiscount.get(imsi);
			else
				l = new ArrayList<Map<String,Object>>();

			Map<String,Object> m = new HashMap<String,Object>();
			m.put("SERVICEID", rs.getString("SERVICEID"));
			m.put("ID", rs.getString("DISCOUNT_ID"));
			m.put("USED", rs.getFloat("used_amount"));
			m.put("LEFT", rs.getFloat("left_amount"));
			m.put("TYPE", rs.getString("DISCOUNT_TYPE"));
			m.put("PRIORITY", rs.getInt("PRIORITY"));
			
			l.add(m);
			
			userDiscount.put(imsi, l);
		}
	}
	
	public void insertCDR() throws SQLException {
		logger.info("insertCDR...");
		int count = 0;
		
		String sql = null;
		
		for(Map<String, Object>  m: newCDRList) {
			Map<Integer, String> data = (Map<Integer, String>) m.get("data");
				sql = ""
					+ "insert into TAPOUT_CDR("
					+ "CDR_VERSION,		CDR_INDENTIFIER,	CDR_GROUP_ID,	CDR_SEQUENCE,		END_FLAG,		CALL_DIRECTION,		CDR_TYPE,"
					+ "CALLING_NUMBER,	CALLED_NUMBER,		STARTTIME,		ENDTIME,			CALLING_IP,		CALL_FORWARD_TYPE,	SERVICE_TYPE,"
					+ "CALLING_AREA,	CALLEE_AREA,		IMSI,			START_TIME_OFFSET,	END_TIME_OFFSET,	NE_IDENTIFIER,	DIALLED_DIGITS,"
					+ "CONTNET_CODE,	PROVIDER_CODE,		INPUT_VOLUME,	OUTPUT_VOLUME,		CHARGING_ID,"
					+ "CHARGE_TAG,		TOTAL_CHARGE,		DISCOUNT_CHARGE,	FINAL_CHARGE,	DISCOUNT_USAGE,	DISCOUNT_ITEMS) "
					+"values ( "
					+ ""+data.get(1)+",	'"+data.get(3)+"',		'"+data.get(4)+"',	"+data.get(5)+",		"+data.get(6)+",	"+data.get(8)+",	"+data.get(9)+","
					+ "'"+data.get(10)+"',	'"+data.get(11)+"',		'"+data.get(12)+"',	'"+data.get(13)+"',		'"+data.get(14)+"',	"+data.get(29)+",	"+data.get(66)+","
					+ "'"+data.get(128)+"',	'"+data.get(129)+"',		'"+data.get(145)+"',	'"+data.get(147)+"',		'"+data.get(148)+"',	'"+data.get(156)+"',	'"+data.get(157)+"',"
					+ "'"+data.get(75)+"',	'"+data.get(74)+"',		"+data.get(37)+",	"+data.get(36)+",		'"+data.get(415)+"',"
					+ ""+data.get(50000)+",	"+data.get(50001)+",		"+data.get(50002)+",	"+data.get(50003)+",		"+data.get(50004)+",	'"+data.get(50005)+"')";
			
			//logger.info(sql);	
				
			st.addBatch(sql);
			count++;
			
			if(count==1000) {
				logger.info("Execeute Batche...");
				st.executeBatch();
				count=0;
			}
		}
		
		if(count>0) {
			logger.info("Execeute Batche...");
			st.executeBatch();
			count=0;
		}
		
		logger.info("insertCDR end...");
	}
	
	public void updateUserDiscount() throws SQLException {
		logger.info("updateUserDiscount...");
		int count = 0;
		String sql = null;
		for(String imsi : userDiscount.keySet()) {
			count = 0;
			for(Map<String, Object> m : userDiscount.get(imsi)) {
				sql = "update TAPOUT_DISCOUNT_USER_DISCOUNT set LEFT_AMOUNT= "+m.get("LEFT")+" , USED_AMOUNT = "+m.get("USED")+" ,UPDATE_TIME = sysdate " + 
						"where SERVICEID = "+m.get("SERVICEID")+" and DISCOUNT_ID = '"+m.get("ID")+"'";
				
				st.addBatch(sql);
				count++;
				
				if(count==1000) {
					logger.info("Execeute Batche...");
					st.executeBatch();
					count=0;
				}
				
			}
			
			if(count>0) {
				logger.info("Execeute Batche...");
				st.executeBatch();
				count=0;
			}
		}
		
		logger.info("updateUserDiscount end...");
	}
	Boolean testMod = null;
	String defaultMailReceiver = null;
	String errorMailreceiver = null;
	//初始化Logger
	void loadProperties() throws FileNotFoundException, IOException{
		System.out.println("loadProperties...");
		String path="Log4j.properties";
		props.load(new   FileInputStream(path));
		PropertyConfigurator.configure(props);
		
		testMod = !"false".equals(props.getProperty("TestMod").trim());
		defaultMailReceiver = props.getProperty("DefaultMailReceiver","ranger.kao@sim2travel.com,k1988242001@gmail.com").trim();
		errorMailreceiver = props.getProperty("ErrorMailReceiver",defaultMailReceiver).trim();
		//dayExecuteTime = props.getProperty("dayExecuteTime").trim();
		//workDir = props.getProperty("workdir").trim();

		System.out.println("loadProperties Success!");
	}
	
	void initialLog4j(){
		logger =Logger.getLogger(TapoutDiscount.class);
		
		logger.info("testMod:"+testMod);
		//logger.info("dayExecuteTime:"+dayExecuteTime);
		logger.info("errorMailreceiver:"+errorMailreceiver);
		logger.info("Logger Load Success!");
	}
	
	//建立連線
	Long connectTime = System.currentTimeMillis();
	void createConnection() throws SQLException, ClassNotFoundException{
		logger.info("createConnection...");
		connectTime = System.currentTimeMillis();
		
		String url,DriverClass,UserName,PassWord;
		
		url=props.getProperty("Oracle.URL")
				.replace("{{Host}}", props.getProperty("Oracle.Host"))
				.replace("{{Port}}", props.getProperty("Oracle.Port"))
				.replace("{{ServiceName}}", (props.getProperty("Oracle.ServiceName")!=null?props.getProperty("Oracle.ServiceName"):""))
				.replace("{{SID}}", (props.getProperty("Oracle.SID")!=null?props.getProperty("Oracle.SID"):""));
		
		DriverClass = props.getProperty("Oracle.DriverClass");
		UserName = props.getProperty("Oracle.UserName");
		PassWord = props.getProperty("Oracle.PassWord");
		
		Class.forName(DriverClass);
		conn = DriverManager.getConnection(url, UserName, PassWord);
		conn.setAutoCommit(false);
		st = conn.createStatement();
		

		
		/*url=props.getProperty("mBOSS.URL")
				.replace("{{Host}}", props.getProperty("mBOSS.Host"))
				.replace("{{Port}}", props.getProperty("mBOSS.Port"))
				.replace("{{ServiceName}}", (props.getProperty("mBOSS.ServiceName")!=null?props.getProperty("mBOSS.ServiceName"):""))
				.replace("{{SID}}", (props.getProperty("mBOSS.SID")!=null?props.getProperty("mBOSS.SID"):""));
		
		DriverClass = props.getProperty("mBOSS.DriverClass");
		UserName = props.getProperty("mBOSS.UserName");
		PassWord = props.getProperty("mBOSS.PassWord");
		
		Class.forName(DriverClass);
		conn2 = DriverManager.getConnection(url, UserName, PassWord);
		st2 = conn2.createStatement();*/
		
		logger.info("createConnection Success!");
	}
	
	void closeConnection(){
		logger.info("closeConnection...");
		if(rs!=null)
			try {
				rs.close();
			} catch (SQLException e) {	}
		
		rs = null;
		
		
		if(st!=null)
			try {
				st.close();
			} catch (SQLException e) {
			}

		if(conn!=null)
			try {
				conn.close();
			} catch (SQLException e) {
			}	
		
		
		st = null;
		conn = null;
		
/*		if(st2!=null)
			try {
				st2.close();
			} catch (SQLException e) {
			}

		if(conn2!=null)
			try {
				conn2.close();
			} catch (SQLException e) {
			}	
		st2=null;
		conn2 = null;*/
		
		logger.info("closeConnection Success!");
	}
	
	
	void errorHandle(Exception e){
		errorHandle(null,e);
	}
	void errorHandle(String content){
		errorHandle(content,null);
	}
	
	void errorHandle(String content,Exception e){
		String errorMsg = null;
		if(e!=null){
			logger.error(content, e);
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			errorMsg=s.toString();
		}
		sendErrorMail(content+"\n"+errorMsg);
	}
	
	void sendErrorMail(String content){
		sendMail(content,errorMailreceiver);
	}

	void sendMail(String mailContent,String mailReceiver){
		String mailSubject="TapoutDiscount Error";
		String mailSender="TapoutDiscount_Server";
		
		String [] cmd=new String[3];
		cmd[0]="/bin/bash";
		cmd[1]="-c";
		cmd[2]= "/bin/echo \""+mailContent+"\" | /bin/mail -s \""+mailSubject+"\" -r "+mailSender+" "+mailReceiver;
	
		try{
			Process p = Runtime.getRuntime().exec (cmd);
			p.waitFor();
			if(logger!=null)
				logger.info("send mail cmd:"+cmd[2]);
			System.out.println("send mail cmd:"+cmd[2]);
		}catch (Exception e){
			if(logger!=null)
				logger.info("send mail fail:"+cmd[2]);
			System.out.println("send mail fail:"+cmd[2]);
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
	}
	

}
