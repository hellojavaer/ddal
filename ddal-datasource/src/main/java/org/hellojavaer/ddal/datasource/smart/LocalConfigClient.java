package org.hellojavaer.ddal.datasource.smart;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.Resource;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 09/09/2017.
 */
public class LocalConfigClient implements ConfigClient {

    private String location;
    private String content;

    public LocalConfigClient(String location) throws IOException {
        this.location = location;
        ApplicationContext context;
        if (location.startsWith("classpath:") || location.startsWith("classpath*:")) {
            context = new ClassPathXmlApplicationContext();
        } else {
            context = new FileSystemXmlApplicationContext();
        }
        StringBuilder sb = new StringBuilder();
        Resource resource = context.getResource(location);
        File file = resource.getFile();
        Scanner scanner = new Scanner(file);
        try {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                sb.append(line).append("\n");
            }
        } finally {
            closeIO(scanner);
        }
        this.content = sb.toString();
    }

    private void closeIO(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignore) {

            }
        }
    }

    @Override
    public String get() {
        return this.content;
    }
}
