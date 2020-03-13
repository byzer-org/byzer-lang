package tech.mlsql.template;

/**
 * 11/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public class SQLSnippetTemplate {
    public String get(String templateName, String... params) {
        SQLSnippetTemplateForScala tfs = new SQLSnippetTemplateForScala();
        return tfs.get(templateName, params);
    }
}
