package streaming.common.zk;

import java.util.ArrayList;
import java.util.List;

/**
 * 7/7/16 WilliamZhu(allwefantasy@gmail.com)
 */
public class Path {

    private List<String> detail ;

    private String split = "/";

    public Path(){}
    public Path(String path){

        String arr[] = path.split("/+");

        detail = new ArrayList<String>();
        for(String p : arr){
            if(p != null && !p.isEmpty()){

                detail.add(p);
            }
        }

    }

    public List<String> getDetail() {
        return detail;
    }

    public void setDetail(List<String> detail) {
        this.detail = detail;
    }



    public Path getParentPath(){

        if(detail == null){
            return null;
        }
        Path path = new Path();

        if(detail.size() > 1){
            path.setDetail(detail.subList(0, detail.size() - 1));
        }

        return path;
    }

    public String getPathString(){

        if(detail == null){
            return split;
        }
        StringBuilder r = new StringBuilder();


        for(String p : detail){

            if(p != null && !p.isEmpty())
                r.append(split).append(p);

        }
        return r.toString();
    }

    public String getName(){

        if(detail == null){
            return split;
        }
        return detail.get(detail.size() - 1);
    }

    private static boolean test(){

        Path p = new Path("/video/_index/pid/");
        boolean flag =true;
        int i = 0;
        String arr[] = {"/video/_index/pid", "/video/_index", "/video", "/"};
        while(p != null){

            if(!arr[i].equals(p.getPathString())){
                flag = false;
            }

            p = p.getParentPath();
            i++;
        }
        return flag;
    }

    public static void main(String[] args) {


        Path p = new Path("/video/_index/pid");
        System.out.println(p.getPathString());
        System.out.println(p.getParentPath().getPathString());
        System.out.println(p.getPathString());

    }
}