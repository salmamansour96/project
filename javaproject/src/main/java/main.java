import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.vector.BaseVector;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;

public class main {

    public static void main(String[] args) throws IOException {
        // display the dataset
        String Path="C:\\Users\\MMansour\\Desktop\\archive\\Wuzzuf_Jobs.csv";
        DAO data=new DAO();
        DataFrame dataset=data.readCSV(Path);
        /*
        System.out.println("----------------Dataset-------------");
        System.out.println(dataset);
        System.out.println("----------------Dataset Structure----------");
        System.out.println(dataset.structure());
        System.out.println("----------------Dataset Summary-------------");
        System.out.println(dataset.summary());
         */

        // Data Preprocessing
        // 1. Remove Arabic Words
        BaseVector title_col=dataset.column("Title");
        List<String> col_title_list = Arrays.asList(title_col.toStringArray());
        List<String>Filtered_title=data.filter_regex(col_title_list,"\\W");

        BaseVector skill_col=dataset.column("Skills");
        List<String> col_skill_list = Arrays.asList(skill_col.toStringArray());
        List<String>Filtered_skill=data.filter_regex(col_skill_list,"[^a-zA-Z,]");


        //2. Remove location from the title if it exists
        BaseVector location_col=dataset.column("Location");
        List<String> col_location_list = Arrays.asList(location_col.toStringArray());
        List<String>preprocessed_title=data.remove_substring_drop_empty(Filtered_title,col_location_list);

        // fix abbreviations in the title
        // sr. senior //QC // Quality Control
        List<String>processed_title=data.process_abbreviation(preprocessed_title);

        //process Years of Exp
        BaseVector years_of_exp=dataset.column("YearsExp");
        List<String> col_years_exp_list = Arrays.asList(years_of_exp.toStringArray());

        //3.drop years of exp if it exists in skills
        List<String> processed_skill=data.remove_substring_drop_empty(Filtered_skill,col_years_exp_list.stream().map(s->s.replaceAll("\\s+","")).collect(Collectors.toList()));

        //4. set null  for null Yrs of Exp
        List<String>processed_exp=data.null_values(col_years_exp_list);

        //5.get columns of company , type , country , level
        BaseVector company=dataset.column("Company");
        List<String> col_company_list = Arrays.asList(company.toStringArray());
        BaseVector type=dataset.column("Type");
        List<String> col_type_list = Arrays.asList(type.toStringArray());
        BaseVector country=dataset.column("Country");
        List<String> col_country_list = Arrays.asList(country.toStringArray());
        BaseVector level=dataset.column("Level");
        List<String> col_level_list =  Arrays.asList(level.toStringArray());




        // convert lists to DataFrame

        //Create instances of a POJO class


        List<POJO>job_list =data.store_data_in_objects(processed_title,col_company_list,col_location_list,col_type_list,col_level_list,processed_exp,col_country_list,processed_skill);


        SparkConf conf = new SparkConf ().setAppName ("jobs").setMaster ("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
        Dataset<Row> df = sqlContext.createDataFrame(job_list, POJO.class);

       // df.show(10);
        StringIndexer indexer = new StringIndexer()
                .setInputCol("years_of_experience")
                .setOutputCol("years_of_experienceIndex");

        Dataset<Row> indexed = indexer.fit(df).transform(df);
        indexed.show();
        
        
       // Dataset<Row>y=  indexed.select("company","title","years_of_experienceIndex");
        //y.show();
        
        
        StringIndexer indexer_title= new StringIndexer()
                .setInputCol("title")
                .setOutputCol("title_Index");
        
        StringIndexer indexer_company = new StringIndexer()
                .setInputCol("company")
                .setOutputCol("company_Index");
        
        Dataset<Row> indexed1 = indexer_title.fit(df).transform(df);
        Dataset<Row> indexed2 = indexer_company.fit(indexed1).transform(indexed1);
        
         Dataset<Row>ya=  indexed2.select("company_Index","title_Index");
        ya.show();
        
        
        
        
        VectorAssembler assembler = new VectorAssembler()
        		  .setInputCols(new String[]{"company_Index", "title_Index"})
        		  .setOutputCol("features");
        Dataset<Row> output = assembler.transform(ya);
        
        KMeans kmeans = new KMeans().setK(5).setSeed(1L);
        
        KMeansModel model = kmeans.fit(output);
        Dataset<Row> predictions = model.transform(output);

     // Evaluate clustering by computing Silhouette score
     ClusteringEvaluator evaluator = new ClusteringEvaluator();

     double silhouette = evaluator.evaluate(predictions);
     System.out.println("Silhouette with squared euclidean distance = " + silhouette);

     // Shows the result.
     
        
        



        /*

        SparkSession spark=SparkSession.builder()
                .master("local")
                .appName("Word Count")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("fieldx1", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fieldx2", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fieldx3", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);
        List<BaseVector> dt = new ArrayList<>();
        dt.add((BaseVector) RowFactory.create("a","b","c"));
        dt.add((BaseVector) RowFactory.create("c","d","c"));

        List<BaseVector>dd=dt;
        System.out.println(dd);




         */




       // df.show(55);
        //drop null values
        /* null is dropped using a condition when converting lists to dataset in function store data in objects*/
        //drop duplicates
       // df.dropDuplicates();













        data.count_jobs(col_company_list);
       data.counting_job_titles(processed_title);
       data.counting_areas(col_location_list);
       data.counting_skills(processed_skill);
      




    }

}
