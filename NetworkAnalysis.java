package young.github.network;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


import au.com.bytecode.opencsv.CSVReader;

public class NetworkAnalysis {
	public static int isUser(String user) throws IOException, Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		conn = JDBCUtils.getConnection();
		st = conn.createStatement();
		int tag = 0;
		int count = 0;
		try{
		String mysql = String.format("select count(*) from users where login = \"%s\";",user);
//		System.out.println(mysql);
		rs = st.executeQuery(mysql);
		while(rs.next())
		{
			count = rs.getInt(1);
			if(count!=0)
			{
				tag =1;
				break;
			}
		}
		}finally{
			JDBCUtils.free(rs, st, conn);
		//	Thread.sleep(10);
		}
		return tag;
	}
	
	public static void insertAt(String query) throws IOException, Exception{
		Connection conn = null;
		Statement st = null;
		conn = JDBCUtils.getConnection();
		st = conn.createStatement();
		try{
		String mysql = String.format("insert into at_users(pr_id,repo,ori_user,ref_user) values(%s);",query);
	//	System.out.println(mysql);
		System.out.println("@@@");
		st.execute(mysql);}finally{
		JDBCUtils.free(null, st, conn);
		Thread.sleep(5);
		}
    }
	
	public static void findAt(String pr_id,String repo,String user,String text_in) throws Exception{
		int tag = 0;
		String ref_user = null;
		String text = text_in;
		while(text.contains("@"))
		{
			int begin = text.indexOf("@");
			int end = text.indexOf(" ", begin);
			if(end != -1)
			{
				ref_user = text.substring(begin+1,end);
			}	
			else
			{
				ref_user = text.substring(begin+1);	
			//	break;
			}
			String login = ref_user.replaceAll("\"", "");
			while(login.endsWith("\\"))
			{
				login = login.substring(0,login.length()-1);
			}
			tag = isUser(login);
			if(tag==1)
			{
				
				String buff = String.format("\"%s\",\"%s\",\"%s\",\"%s\"",pr_id,repo,user,login);
				insertAt(buff);	
			}
			if(end != -1)
			{
				text = text.substring(end+1);
			}
			else
			{
				break;
			}
		}
	}
	public static void findBody(String pr_id,String repo,String table) throws IOException, Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		conn = JDBCUtils.getConnection();
		st = conn.createStatement();
		String body = null;
		String user =null;
		try{
		String mysql = String.format("select user,body from %s where pr_id = \"%s\";",table,pr_id);
		rs = st.executeQuery(mysql);
		while(rs.next())
		{
			user = rs.getString(1);
			body = rs.getString(2);
			findAt(pr_id,repo,user,body);
		}
		}finally{
			JDBCUtils.free(rs, st, conn);
			Thread.sleep(5);
		}
	}
	public static void findComment(String pr_id,String repo) throws IOException, Exception{
		findBody(pr_id,repo,"pr_comments");
		findBody(pr_id,repo,"commit_comments");
		findBody(pr_id,repo,"issue_comments");
	}
	
	public static void atAnalysis() throws Exception{
		int count=351250;
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		conn = JDBCUtils.getConnection();
		st = conn.createStatement();
		try{
		String mysql = String.format("select pr_id,repo,user,title,body from pull_requests where id>=%d;",count);
		//System.out.println(mysql);
		rs = st.executeQuery(mysql);
		while(rs.next())
		{
			System.out.println(count);
			count++;
			String pr_id = rs.getString(1);
			String repo = rs.getString(2);
			String user = rs.getString(3);
			String title =  rs.getString(4);
			String body =  rs.getString(5);
			
			String text = String.format("%s %s",title,body);
			
			findAt(pr_id,repo,user,text);
			findComment(pr_id,repo);
		}
		}finally{
		JDBCUtils.free(rs, st, conn);
		//Thread.sleep(10);
		}
	}
	public static void main(String arg[]) throws Exception{
		atAnalysis();
	}

}
