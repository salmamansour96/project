import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import smile.data.DataFrame;
import smile.data.Dataset;
import smile.io.Read;

import java.awt.*;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
public class DAO {

    private DataFrame job_dataframe;

    public DAO() {

    }

    //counting skills
    public void counting_skills(List<String> jobs)
    {  List<String> all=new ArrayList<String>();
      //split the row to skills
      for (String str1 : jobs)
      {
      for (String s : str1.split(","))
    
         {all.add(s);
         System.out.println(s);
      
          }       
    	
    	
    }
    
    Map<String, Long> result =
        	    all.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting())); 

   LinkedHashMap<String, Long> mp = result.entrySet().stream()
   .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue,(oldValue,newValue)->
   oldValue,LinkedHashMap::new));
    List<String> skills_list = new ArrayList<String>();

        List<Long> Counts=new ArrayList<Long>();
        int c=0;
        for (Map.Entry<String, Long> entry : mp.entrySet()) {
            Long count = entry.getValue();
            String skill = entry.getKey();
            if (c<6)
            {skills_list.add(skill);
             Counts.add(count);
             c=c+1;
            }
            
            System.out.println("Skill: " + skill + ", Skill count : " + count);
        }
        
        
        CategoryChart chart = new CategoryChartBuilder ().width (1024).height (768).title ("Most popular Skills").xAxisTitle ("Skill").yAxisTitle
        		("Count").build ();
        		// 2.Customize Chart
        		//chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        		chart.getStyler ().setHasAnnotations (true);
        		chart.getStyler ().setStacked (true);
        		// 3.Series
        		
        		chart.addSeries ("Skills count", skills_list, Counts);
        		// 4.Show it
        		new SwingWrapper (chart).displayChart ();
        		
    
    
    }
    //counting areas
    public void counting_areas(List<String> areas)
    {
    	Map<String, Long> result =
                areas.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        

       LinkedHashMap<String, Long> mp = result.entrySet().stream()
       .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue,(oldValue,newValue)->
       oldValue,LinkedHashMap::new));
        List<String> areas_list = new ArrayList<String>();
        
       
    
        List<Long> Counts=new ArrayList<Long>();
        int c=0;
        for (Map.Entry<String, Long> entry : mp.entrySet()) {
            Long count = entry.getValue();
            String area = entry.getKey();
            if (c<6)
            {areas_list.add(area);
             Counts.add(count);
             c=c+1;
            }
            
            System.out.println("Location: " + area + ", Area count : " + count);
        }
        
        
        CategoryChart chart = new CategoryChartBuilder ().width (1024).height (768).title ("Most popular Areas").xAxisTitle ("Area").yAxisTitle
        		("Count").build ();
        		// 2.Customize Chart
        		//chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        		chart.getStyler ().setHasAnnotations (true);
        		chart.getStyler ().setStacked (true);
        		// 3.Series
        		
        		chart.addSeries ("Area's count", areas_list, Counts);
        		// 4.Show it
        		new SwingWrapper (chart).displayChart ();
        		
    	
    	
    }
    //counting job titles
    public void counting_job_titles(List<String> titles) {
    	Map<String, Long> result =
        	    titles.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));

       LinkedHashMap<String, Long> mp = result.entrySet().stream()
       .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue,(oldValue,newValue)->
       oldValue,LinkedHashMap::new));
        List<String> Titles_list = new ArrayList<String>();
        
       
    
        List<Long> Counts=new ArrayList<Long>();
        int c=0;
        for (Map.Entry<String, Long> entry : mp.entrySet()) {
            Long count = entry.getValue();
            String title = entry.getKey();
            if (c<6)
            {Titles_list.add(title);
             Counts.add(count);
             c=c+1;
            }
            
            System.out.println("Title: " + title + ", Title count : " + count);
        }
        
        
        CategoryChart chart = new CategoryChartBuilder ().width (1024).height (768).title ("Most popular Titles").xAxisTitle ("Title").yAxisTitle
        		("Count").build ();
        		// 2.Customize Chart
        		//chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        		chart.getStyler ().setHasAnnotations (true);
        		chart.getStyler ().setStacked (true);
        		// 3.Series
        		
        		chart.addSeries ("Titles's count", Titles_list, Counts);
        		// 4.Show it
        		new SwingWrapper (chart).displayChart ();
        		
        		
    	
    	
    }
    public DataFrame readCSV(String path) {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
        DataFrame df = null;

        try {
            df = Read.csv(path, format);
        } catch (URISyntaxException | IOException var5) {
            var5.printStackTrace();
        }

        this.job_dataframe = df;
        return df;
    }

    // filter according to Regex
    public List<String> filter_regex(List<String> ls, String Regex) {
        List<String> filtered = ls.stream()
                .map(x -> x.replaceAll(Regex, "")).collect(Collectors.toList());
        return filtered;
    }

    //fix abbreviations for titles

    public List<String> process_abbreviation(List<String> title) {

        Hashtable<String, String> abb_ls = new Hashtable<>();
        abb_ls.put("Sr", "Senior");
        abb_ls.put("Jr", "Junior");
        abb_ls.put("QC", "QualityControl");

        int counter = 0;
        for (String t : title) {
            if (t.length()>2){
                for (String abb : abb_ls.keySet()) {
                        if (t.substring(0, 2).equals(abb)) {
                            title.set(counter, t.replaceAll(abb, abb_ls.get(abb)));
                        }

                    }
            }
            counter += 1;
        }
        return title;
    }

    // remove location if it exists in title from the title and drop the column without title
    // remove Years of Exp if it exists in skills
    public List<String> remove_substring_drop_empty(List<String> T, List<String> L) {
        for (int i = 0; i < L.size(); i++) {
            List<String> location_split = Arrays.asList(L.get(i).split(" "));
            for (String loc : location_split) {
                if (T.get(i).contains(loc)) {
                    T.set(i, T.get(i).replaceAll(loc, ""));
                    break;
                }
            }
        }
        return T;
    }

    // set null in YearsExp to be dropped later
    public List<String> null_values(List<String> exp) {
        List<String> filtered = exp.stream()
                .map(x -> x.replaceAll("null Yrs of Exp", "")).collect(Collectors.toList());
        return filtered;
    }

    // function to sort hashmap by values
    public static LinkedHashMap<String, Long> sortByValue(Map<String, Long> k) {
        // Create a list from elements of HashMap
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();
        k.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        return reverseSortedMap;
    }

    // 4. get counting for jobs in each company
    public void count_jobs(List<String> jobs) {

        Map<String, Long> result =
                jobs.stream().collect(
                        Collectors.groupingBy(e->e, Collectors.counting()));

        LinkedHashMap<String, Long> mp = sortByValue((HashMap<String, Long>) result);

        for (Map.Entry<String, Long> entry : mp.entrySet()) {
            Long count = entry.getValue();
            String company = entry.getKey();
            System.out.println("Company: " + company + "| Jobs count: " + count);
        }

        PieChart chart = new PieChartBuilder().width(800).height(600).title(getClass().getSimpleName()).build();
        Color[] sliceColors = new Color[]{new Color(180, 68, 50), new Color(130, 105, 120)};
        chart.getStyler().setSeriesColors(sliceColors);
        int counter=0;
        for (Map.Entry<String, Long> entry : mp.entrySet()) {
        chart.addSeries(entry.getKey(), result.get(entry.getKey()));
        if (counter>=5)
            break;
        else
            counter++;
        }
        new SwingWrapper(chart).displayChart();


    }

    public List<POJO> store_data_in_objects(List<String>processed_title,List<String>col_company_list,List<String>col_location_list,List<String>col_type_list,List<String>col_level_list,List<String>processed_exp,List<String>col_country_list,List<String>processed_skill){

        List<POJO>job_list = new ArrayList<>();
        for(int i=0;i<processed_title.size();i++){
            // condition to remove null
            if(!(processed_title.get(i).equals("")||processed_exp.get(i).equals("")))
                job_list.add(new POJO(processed_title.get(i),col_company_list.get(i),col_location_list.get(i),col_type_list.get(i),col_level_list.get(i),processed_exp.get(i),col_country_list.get(i),processed_skill.get(i)));

        }
        return  job_list;
    }

    // Read csv file and return a list containing Objects
    /*
    public List<POJO> getjobsFromCsvFile(String path) throws IOException {
        List<POJO> jobs = new ArrayList<POJO> ();
        Path csvfile= Paths.get(path);
        BufferedReader reader = Files.newBufferedReader((java.nio.file.Path) csvfile, StandardCharsets.UTF_8);
        CSVFormat csv=CSVFormat.RFC4180.withHeader();
        CSVParser parser=csv.parse(reader);
        Iterator<CSVRecord>it= parser.iterator();
        it.forEachRemaining(rec->{
            String title=rec.get("Title");
            String company=rec.get("Company");
            String location=rec.get("Location");
            String type=rec.get("Type");
            String level=rec.get("Level");
            String years_of_experience=rec.get("YearsExp");
            String skills=rec.get("Skills");

            POJO job=new POJO(title,company,location,type,level,years_of_experience,skills);
            jobs.add(job);
        });

        return jobs;
    }
*/


}