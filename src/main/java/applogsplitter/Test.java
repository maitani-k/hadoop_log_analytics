package applogsplitter;

import org.apache.commons.lang.StringUtils;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String s = "/main.php/quest/result";
		
		String[] a = StringUtils.split(s, "?", 2);

		if(a.length == 2){
			System.out.println(a[0]);
			System.out.println(a[1]);
		}else{
			System.out.println(a[0]);
			System.out.println("");			
		}
	}

}
