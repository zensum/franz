package franz.runtime

import franz.Worker
import org.xeustechnologies.jcl.JarClassLoader

private fun loaderWithJar(path: String) = JarClassLoader().apply {
    add(path)
}

fun getWorker(path: String, className: String) =
    loaderWithJar(path)
        .loadClass(className)
        .newInstance() as Worker
