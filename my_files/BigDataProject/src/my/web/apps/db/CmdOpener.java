package my.web.apps.db;

public class CmdOpener {
	public void open() {
		try
		{
			Runtime.getRuntime().exec(new String[] {"cmd", "/k", "Start"});
			System.out.println("Cmd is running!");
		}
		catch (Exception e)
		{
			System.out.println("Error opening cmd!");
			e.printStackTrace();
		}
	}

}
