package young.github.at;

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

import young.github.jing.JDBCUtils;


import au.com.bytecode.opencsv.CSVReader;

public class AtAnalysis {
	public static int isUser(String user) throws IOException, Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		conn = JDBCUtils.getConnection();
		st = conn.createStatement();
		int tag = 0;
		int count = 0;
		String login = user.replaceAll("\"", "");
		while(login.endsWith("\\"))
		{
			login = login.substring(0,login.length()-1);
		}
		String mysql = String.format("select count(*) from users where login = \"%s\";",login);
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
		conn.close();
		return tag;
	}
	public static int findAt(String body) throws Exception{
		int tag = 0;
		String user = null;
		while(body.contains("@"))
		{
			int begin = body.indexOf("@");
			int end = body.indexOf(" ", begin);
			if(end != -1)
			{
				user = body.substring(begin+1,end);
			}	
			else
			{
				user = body.substring(begin+1);	
			//	break;
			}
		    
			tag = isUser(user);
			if(tag==1)
			{
				break;//return user
			}
			else
			{
				if(end != -1)
				{
					body = body.substring(end+1);
				}
				else
				{
					break;
				}
			}
		}
		return tag;
	}
	public static int findBody(String pr_id,String table) throws IOException, Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		conn = JDBCUtils.getConnection();
		st = conn.createStatement();
		int tag = 0;
		String body = null;
		String mysql = String.format("select body from %s where pr_id = \"%s\";",table,pr_id);
		rs = st.executeQuery(mysql);
		while(rs.next())
		{
			body = rs.getString(1);
			tag  = findAt(body);
			if(tag == 1)
			{
				break;
			}
		}
		conn.close();
		return tag;
	}
	public static int[] findComment(String pr_id) throws IOException, Exception{
		int[] tag = {0,0,0};
		int pr_comment_tag = findBody(pr_id,"closed_pr_comments_2");
		int commit_comment_tag = findBody(pr_id,"closed_pr_commit_comments_2");
		int issue_comment_tag = findBody(pr_id,"closed_pr_issue_comments_2");
		if(issue_comment_tag==1)
		{
			tag[0] = 1;
		}
		if(pr_comment_tag==1)
		{
			tag[1] = 1;
		}
		if(commit_comment_tag==1)
		{
			tag[2] = 1;
		}
		return tag;
	}
	public static int findMember(String repo,String user) throws IOException, Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		conn = JDBCUtils.getConnection();
		st = conn.createStatement();
		int tag = 0;
		int count = 0;
		String mysql = String.format("select count(*) from repo_members_1 where repo = \"%s\" and user = \"%s\";",repo,user);
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
		conn.close();
		return tag;
	}
	public static void atAnalysis(String inpath) throws Exception{
		FileInputStream reader = new FileInputStream(inpath);
		InputStreamReader is = new InputStreamReader(reader,"UTF-8");
		CSVReader cr = new CSVReader(is);
		
		String[] s = null;
		cr.readNext();
		int number = 0;
		while((s = cr.readNext()) != null){
			System.out.println(number);
			number++;
	//		if(number>=187331)
	//		{
            String pr_id = s[0];
            String user = s[1];
            String title = s[2];
            String body = s[3];
            String open_time = s[4];
            String closed_time = s[5];
            int total_time = Integer.valueOf(s[6]);  
            int commits_count = Integer.valueOf(s[7]);
            int comments_count = Integer.valueOf(s[8]);
            int comment_time = Integer.valueOf(s[9]);
            
            int to  = pr_id.indexOf("+");
            String repo = pr_id.substring(0,to);
            System.out.println(repo);
            int member = findMember(repo,user);
            int[] at_array = findComment(pr_id);	
            int issue_at = at_array[0];
            int pr_at = at_array[1];
            int commit_at = at_array[2];
            int title_at = findAt(title);
            int body_at = findAt(body);
            int at = 0;
            if(issue_at==0&&pr_at==0&&commit_at==0&&title_at==0&&body_at==0)
            {
            	at = 0;
            }
            else
            {
            	at = 1;
            }
            /*insert data*/
            Connection conn = null;
    		Statement st = null;
    		conn = JDBCUtils.getConnection();
    		st = conn.createStatement();
    		String mysql = String.format("insert into pull_requests_6_0(pr_id,repo,user,title,body,created_at,closed_at,total_time,"
    				+ "commits_count,comments_count,comment_time,member,issue_at,pr_at,commit_at,title_at,body_at,at)"
    				+ " values (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%d,%d,%d,\"%s\",%d,%d,%d,%d,%d,%d,%d);",
    				pr_id,repo,user,title,body,open_time,closed_time,total_time,commits_count,comments_count,comment_time,
    				member,issue_at,pr_at,commit_at,title_at,body_at,at);
    		st.execute(mysql);
    		conn.close();
		}	
	}
	public static void main(String arg[]) throws Exception{
		String inpath = "H://Young_Final//New Statistic//pull_requests_4_0.csv";
	//	String outpath = "H://PR_Final//young//results_issue_comments_at_origin.csv";
		atAnalysis(inpath);
	}

}
