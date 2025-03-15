/*
 *    Copyright 2009-2024 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.AutoMappingUnknownColumnBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  private boolean parsed;

  // xml解析器
  private final XPathParser parser;
  private String environment;
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    this(Configuration.class, reader, environment, props);
  }

  public XMLConfigBuilder(Class<? extends Configuration> configClass, Reader reader, String environment,
                          Properties props) {
    this(configClass, new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    this(Configuration.class, inputStream, environment, props);
  }

  public XMLConfigBuilder(Class<? extends Configuration> configClass, InputStream inputStream, String environment,
                          Properties props) {
    this(configClass, new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  private XMLConfigBuilder(Class<? extends Configuration> configClass, XPathParser parser, String environment,
                           Properties props) {
    super(newConfig(configClass));
    ErrorContext.instance().resource("SQL Mapper Configuration");
    this.configuration.setVariables(props);
    this.parsed = false;
    this.environment = environment;
    this.parser = parser;
  }

  public Configuration parse() {
    if (parsed) {
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    parsed = true;
    XNode configurationNode = parser.evalNode("/configuration");
    parseConfiguration(configurationNode);
    return configuration;
  }

  /**
   * 解析 "/configuration"节点下的子节点信息，然后将解析的结果设置到Configuration对象中
   *
   * @param root
   */
  private void parseConfiguration(XNode root) {
    try {
      // issue #117 read properties first
      // 1.首先处理properties 节点
      propertiesElement(root.evalNode("properties"));
      // 6.settings
      Properties settings = settingsAsProperties(root.evalNode("settings"));
      loadCustomVfsImpl(settings);
      loadCustomLogImpl(settings);
      // 2.处理typeAliases
      typeAliasesElement(root.evalNode("typeAliases"));
      // 3.处理插件
      pluginsElement(root.evalNode("plugins"));
      // 4.处理objectFactory
      objectFactoryElement(root.evalNode("objectFactory"));
      // 5.objectWrapperFactory
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
      reflectorFactoryElement(root.evalNode("reflectorFactory"));
      settingsElement(settings);
      // read it after objectFactory and objectWrapperFactory issue #631
      // 7.处理environments
      environmentsElement(root.evalNode("environments"));
      // 8.database
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));
      // 9.typeHandlers
      typeHandlersElement(root.evalNode("typeHandlers"));
      // 10.mappers
      mappersElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  private Properties settingsAsProperties(XNode context) {
    if (context == null) {
      return new Properties();
    }
    Properties props = context.getChildrenAsProperties();
    // Check that all settings are known to the configuration class
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
    for (Object key : props.keySet()) {
      if (!metaConfig.hasSetter(String.valueOf(key))) {
        throw new BuilderException(
          "The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    return props;
  }

  private void loadCustomVfsImpl(Properties props) throws ClassNotFoundException {
    String value = props.getProperty("vfsImpl");
    if (value == null) {
      return;
    }
    String[] clazzes = value.split(",");
    for (String clazz : clazzes) {
      if (!clazz.isEmpty()) {
        @SuppressWarnings("unchecked")
        Class<? extends VFS> vfsImpl = (Class<? extends VFS>) Resources.classForName(clazz);
        configuration.setVfsImpl(vfsImpl);
      }
    }
  }

  private void loadCustomLogImpl(Properties props) {
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
    configuration.setLogImpl(logImpl);
  }

  /**
   * 解析typeAliases节点
   *
   * @param context
   */
  private void typeAliasesElement(XNode context) {
    if (context == null) {
      return;
    }
    for (XNode child : context.getChildren()) {
      // 如果子节点是package, 那么就获取package节点的name属性， mybatis会扫描指定的package
      if ("package".equals(child.getName())) {
        String typeAliasPackage = child.getStringAttribute("name");
        // TypeAliasRegistry 负责管理别名， 这儿就是通过TypeAliasRegistry 进行别名注册， 下面就会看看TypeAliasRegistry源码
        configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
      } else {
        // 如果子节点是typeAlias节点，那么就获取alias属性和type的属性值
        String alias = child.getStringAttribute("alias");
        String type = child.getStringAttribute("type");
        try {
          Class<?> clazz = Resources.classForName(type);
          if (alias == null) {
            typeAliasRegistry.registerAlias(clazz);
          } else {
            typeAliasRegistry.registerAlias(alias, clazz);
          }
        } catch (ClassNotFoundException e) {
          throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
        }
      }
    }
  }

  private void pluginsElement(XNode context) throws Exception {
    if (context != null) {
      for (XNode child : context.getChildren()) {
        String interceptor = child.getStringAttribute("interceptor");
        Properties properties = child.getChildrenAsProperties();
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).getDeclaredConstructor()
          .newInstance();
        interceptorInstance.setProperties(properties);
        configuration.addInterceptor(interceptorInstance);
      }
    }
  }

  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties properties = context.getChildrenAsProperties();
      ObjectFactory factory = (ObjectFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(properties);
      configuration.setObjectFactory(factory);
    }
  }

  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setObjectWrapperFactory(factory);
    }
  }

  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ReflectorFactory factory = (ReflectorFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setReflectorFactory(factory);
    }
  }

  // 下面就看看解析properties的具体方法
  private void propertiesElement(XNode context) throws Exception {
    if (context == null) {
      return;
    }
    // 将子节点的 name 以及value属性set进properties对象
    Properties defaults = context.getChildrenAsProperties();
    // 获取properties节点上 resource属性的值
    String resource = context.getStringAttribute("resource");
    // 获取properties节点上 url属性的值, resource和url不能同时配置
    String url = context.getStringAttribute("url");
    if (resource != null && url != null) {
      throw new BuilderException(
        "The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
    }
    // 把解析出的properties文件set进Properties对象
    if (resource != null) {
      defaults.putAll(Resources.getResourceAsProperties(resource));
    } else if (url != null) {
      defaults.putAll(Resources.getUrlAsProperties(url));
    }
    // 将configuration对象中已配置的Properties属性与刚刚解析的融合
    // configuration这个对象会装载所解析mybatis配置文件的所有节点元素，以后也会频频提到这个对象
    // 既然configuration对象用有一系列的get/set方法， 那是否就标志着我们可以使用java代码直接配置？
    // 答案是肯定的， 不过使用配置文件进行配置，优势不言而喻
    Properties vars = configuration.getVariables();
    if (vars != null) {
      defaults.putAll(vars);
    }
    // 把装有解析配置propertis对象set进解析器， 因为后面可能会用到
    parser.setVariables(defaults);
    // set进configuration对象
    configuration.setVariables(defaults);
  }

  private void settingsElement(Properties props) {
    configuration
      .setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    configuration.setAutoMappingUnknownColumnBehavior(
      AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    configuration.setDefaultResultSetType(resolveResultSetType(props.getProperty("defaultResultSetType")));
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    configuration.setLazyLoadTriggerMethods(
      stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
    configuration.setShrinkWhitespacesInSql(booleanValueOf(props.getProperty("shrinkWhitespacesInSql"), false));
    configuration.setArgNameBasedConstructorAutoMapping(
      booleanValueOf(props.getProperty("argNameBasedConstructorAutoMapping"), false));
    configuration.setDefaultSqlProviderType(resolveClass(props.getProperty("defaultSqlProviderType")));
    configuration.setNullableOnForEach(booleanValueOf(props.getProperty("nullableOnForEach"), false));
  }

  /**
   * 解析environments节点，并将结果设置到Configuration对象中
   * 注意：创建envronment时，如果SqlSessionFactoryBuilder指定了特定的环境（即数据源）；
   * 则返回指定环境（数据源）的Environment对象，否则返回默认的Environment对象；
   * 这种方式实现了MyBatis可以连接多数据源
   */
  private void environmentsElement(XNode context) throws Exception {
    if (context == null) {
      return;
    }
    if (environment == null) {
      // 解析environments节点的default属性的值
      // 例如: <environments default="development">
      environment = context.getStringAttribute("default");
    }
    // 递归解析environments子节点
    for (XNode child : context.getChildren()) {
      //<environment id="development">, 只有enviroment节点有id属性，那么这个属性有何作用？
      // environments 节点下可以拥有多个 environment子节点
      // 类似于这样： <environments default="development"><environment id="development">...</environment><environment id="test">...</environments>
      // 意思就是我们可以对应多个环境，比如开发环境，测试环境等， 由environments的default属性去选择对应的enviroment
      String id = child.getStringAttribute("id");
      // isSpecial就是根据由environments的default属性去选择对应的enviroment
      if (isSpecifiedEnvironment(id)) {
        // 1.创建事务工厂 TransactionFactory
        // isSpecial就是根据由environments的default属性去选择对应的enviroment
        TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
        DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
        // 创建数据源 DataSource 2025/3/15 15:19 By 少帅
        // enviroment节点下面就是dataSource节点了，解析dataSource节点（下面会贴出解析dataSource的具体方法）
        DataSource dataSource = dsFactory.getDataSource();
        // 3. 构造Environment对象
        Environment.Builder environmentBuilder = new Environment.Builder(id).transactionFactory(txFactory)
          .dataSource(dataSource);
        // 4. 将创建的Environment对象设置到configuration 对象中
        configuration.setEnvironment(environmentBuilder.build());
        break;
      }
    }
  }

  private void databaseIdProviderElement(XNode context) throws Exception {
    if (context == null) {
      return;
    }
    String type = context.getStringAttribute("type");
    // awful patch to keep backward compatibility
    if ("VENDOR".equals(type)) {
      type = "DB_VENDOR";
    }
    Properties properties = context.getChildrenAsProperties();
    DatabaseIdProvider databaseIdProvider = (DatabaseIdProvider) resolveClass(type).getDeclaredConstructor()
      .newInstance();
    databaseIdProvider.setProperties(properties);
    Environment environment = configuration.getEnvironment();
    if (environment != null) {
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
      configuration.setDatabaseId(databaseId);
    }
  }

  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      TransactionFactory factory = (TransactionFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  // 下面看看dataSource节点的解析
  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      // dataSource的连接池
      String type = context.getStringAttribute("type");
      // 子节点 name, value属性set进一个properties对象
      Properties props = context.getChildrenAsProperties();
      // 创建dataSourceFactory
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  /**
   * 解析typeHandlers节点，并将结果设置到Configuration对象中
   *
   * @param context
   */
  private void typeHandlersElement(XNode context) {
    if (context == null) {
      return;
    }
    for (XNode child : context.getChildren()) {
      // 子节点为package时，获取其name属性的值，然后自动扫描package下的自定义typeHandler
      if ("package".equals(child.getName())) {
        String typeHandlerPackage = child.getStringAttribute("name");
        typeHandlerRegistry.register(typeHandlerPackage);
      } else {
        // 子节点为typeHandler时， 可以指定javaType属性， 也可以指定jdbcType, 也可两者都指定
        // javaType 是指定java类型
        // jdbcType 是指定jdbc类型（数据库类型： 如varchar）
        String javaTypeName = child.getStringAttribute("javaType");
        String jdbcTypeName = child.getStringAttribute("jdbcType");
        // handler就是我们配置的typeHandler
        String handlerTypeName = child.getStringAttribute("handler");
        // resolveClass方法就是我们上篇文章所讲的TypeAliasRegistry里面处理别名的方法
        Class<?> javaTypeClass = resolveClass(javaTypeName);
        // JdbcType是一个枚举类型，resolveJdbcType方法是在获取枚举类型的值
        JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
        Class<?> typeHandlerClass = resolveClass(handlerTypeName);
        // 注册typeHandler, typeHandler通过TypeHandlerRegistry这个类管理
        if (javaTypeClass != null) {
          if (jdbcType == null) {
            typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
          } else {
            typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
          }
        } else {
          typeHandlerRegistry.register(typeHandlerClass);
        }
      }
    }
  }

  private void mappersElement(XNode context) throws Exception {
    if (context == null) {
      return;
    }
    for (XNode child : context.getChildren()) {
      if ("package".equals(child.getName())) {
        // 如果mappers节点的子节点是package, 那么就扫描package下的文件, 注入进configuration
        String mapperPackage = child.getStringAttribute("name");
        configuration.addMappers(mapperPackage);
      } else {
        String resource = child.getStringAttribute("resource");
        String url = child.getStringAttribute("url");
        String mapperClass = child.getStringAttribute("class");
        if (resource != null && url == null && mapperClass == null) {
          ErrorContext.instance().resource(resource);
          try (InputStream inputStream = Resources.getResourceAsStream(resource)) {
            // mapper映射文件都是通过XMLMapperBuilder解析
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource,
              configuration.getSqlFragments());
            mapperParser.parse();
          }
        } else if (resource == null && url != null && mapperClass == null) {
          ErrorContext.instance().resource(url);
          try (InputStream inputStream = Resources.getUrlAsStream(url)) {
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url,
              configuration.getSqlFragments());
            mapperParser.parse();
          }
        } else if (resource == null && url == null && mapperClass != null) {
          Class<?> mapperInterface = Resources.classForName(mapperClass);
          configuration.addMapper(mapperInterface);
        } else {
          throw new BuilderException(
            "A mapper element may only specify a url, resource or class, but not more than one.");
        }
      }
    }
  }

  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    }
    if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    }
    return environment.equals(id);
  }

  private static Configuration newConfig(Class<? extends Configuration> configClass) {
    try {
      return configClass.getDeclaredConstructor().newInstance();
    } catch (Exception ex) {
      throw new BuilderException("Failed to create a new Configuration instance.", ex);
    }
  }

}
