package com.alibaba.datax.web;
//import org.apache.tomcat.jdbc.pool.DataSource;
//import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.mapper.MapperScannerConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;

@Component
@Configuration
@ComponentScan(basePackages = {"com.alibaba.datax.web"})
public class SpringJavaConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(SpringJavaConfiguration.class);

	//@Autowired
	//private ApplicationContext applicationContext=new FileSystemXmlApplicationContext("applicationContext.xml");
	/*@Autowired
	private DataSource dataSource;
	@Autowired
	private SqlSessionFactoryBean sqlSessionFactory;
	@Autowired
	private MapperScannerConfigurer mapperScannerConfigurer;*/

	
	@Bean(name="dataSource")
	public DataSource dataSource() {
		//DruidDataSource dataSource=new DruidDataSource();

		PoolProperties poolProperties = new PoolProperties();
		poolProperties.setDriverClassName("com.mysql.jdbc.Driver");
		poolProperties.setUrl("jdbc:mysql://localhost:3306/test?useUnicode=true&amp;characterEncoding=utf-8&amp;autoReconnect=true&amp;failOverReadOnly=false&amp;maxReconnects=10");
		poolProperties.setUsername("root");
		poolProperties.setPassword("root");
		poolProperties.setJmxEnabled(true);
		poolProperties.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
		poolProperties.setRemoveAbandonedTimeout(60);
		poolProperties.setRemoveAbandoned(true);
		poolProperties.setLogAbandoned(false);
		poolProperties.setMinIdle(10);
		poolProperties.setMinEvictableIdleTimeMillis(30000);
		poolProperties.setMaxWait(10);
		poolProperties.setInitialSize(2);
		poolProperties.setMaxActive(10);
		poolProperties.setTimeBetweenEvictionRunsMillis(30000);
		poolProperties.setValidationQuery("SELECT 1");
		poolProperties.setValidationInterval(30000);
		poolProperties.setTestOnReturn(false);
		poolProperties.setTestOnBorrow(true);
		poolProperties.setTestWhileIdle(true);
		poolProperties.setJmxEnabled(true);
		DataSource dataSource = new DataSource();
		dataSource.setPoolProperties(poolProperties);

		/*Properties properties=new Properties();
		properties.setProperty("driverClassName","com.mysql.jdbc.Driver");
		properties.setProperty("url","jdbc:mysql://localhost:3306/test?useUnicode=true&amp;characterEncoding=utf-8&amp;autoReconnect=true&amp;failOverReadOnly=false&amp;maxReconnects=10");
		properties.setProperty("username","root");
		properties.setProperty("password","root");
		properties.setProperty("initialSize","0");
		properties.setProperty("maxActive","20");
		properties.setProperty("minIdle","0");
		properties.setProperty("maxWait","6000");
		properties.setProperty("validationQuery","SELECT 1");
		properties.setProperty("testOnBorrow","false");
		properties.setProperty("testOnReturn","false");
		properties.setProperty("testWhileIdle","true");
		properties.setProperty("timeBetweenEvictionRunsMillis","60000");
		properties.setProperty("minEvictableIdleTimeMillis","25200000");
		properties.setProperty("removeAbandoned","true");
		properties.setProperty("removeAbandonedTimeout","1800");
		properties.setProperty("logAbandoned","true");
		properties.setProperty("filters","mergeStat");

		DataSource dataSource=null;
		try {
			dataSource= DataSourceFactory.parsePoolProperties(properties);
		}
		catch(Exception e)
		{
			LOG.error("初始化druid连接失败: ",e);
		}*/

		return dataSource;
	}
	

	@Bean(name="sqlSessionFactory" )
	public SqlSessionFactoryBean getSqlSesssionFactoryBean(){
		SqlSessionFactoryBean bean=new SqlSessionFactoryBean();
		//bean.setDataSource(dataSource());
		//Resource res[]=new Resource[1];
		//res[0] =new ClassPathResource("mybatis/*/*.xml");
		//Resource resConf=new ClassPathResource("mybatis-config.xml");
		//bean.setMapperLocations(res);
		//bean.setConfigLocation(resConf);

		//Resource res[]=new Resource[1];
		//res[0] =new ClassPathResource("mybatis/*/*.xml");

		//Properties properties=new Properties();
		//properties.setProperty("dataSource","dataSource");
		//properties.setProperty("configLocation","classpath:mybatis-config.xml");
		//bean.setConfigurationProperties(properties);
		//bean.setMapperLocations(res);
		Resource resConf=new ClassPathResource("mybatis-config.xml");
		bean.setConfigLocation(resConf);
		bean.setDataSource(dataSource());
		return bean;

		//return (SqlSessionFactoryBean)applicationContext.getBean("sqlSessionFactory");
	}
	
	@Bean(name="mapperScannerConfigurer")
	public MapperScannerConfigurer getMapperScannerConfigurer(){
		MapperScannerConfigurer conf=new MapperScannerConfigurer();
		conf.setBasePackage("com.alibaba.datax.web.mapper");
		conf.setSqlSessionFactoryBeanName("sqlSessionFactory");
		return conf;

		//return (MapperScannerConfigurer)applicationContext.getBean("mapperScannerConfigurer");
	}

	@Bean(name="transactionManager")
	public DataSourceTransactionManager getDataSourceTransactionManager(){
		DataSourceTransactionManager transactionManager=new DataSourceTransactionManager();
		transactionManager.setDataSource(dataSource());
		return transactionManager;

		//return (MapperScannerConfigurer)applicationContext.getBean("mapperScannerConfigurer");
	}
	
}
