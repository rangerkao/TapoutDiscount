import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TapoutDiscountSetting {
	
	Logger logger = null;
	Properties props = new Properties();
	Connection conn = null;
	Statement st = null;
	ResultSet rs = null;
	
	public static void main(String[] args) {
		new TapoutDiscountSetting();
	}
	
	TapoutDiscountSetting(){

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
				process();
				logger.info("process end...");

				isprocessed = true;
			} catch (SQLException e) {
				errorHandle(e);
			} catch (ClassNotFoundException e) {
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

	public void process() {
		
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
