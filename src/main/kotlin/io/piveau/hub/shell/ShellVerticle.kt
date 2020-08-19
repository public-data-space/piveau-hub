package io.piveau.hub.shell

import io.piveau.json.asJsonObject
import io.vertx.core.json.JsonObject
import io.vertx.ext.shell.ShellService
import io.vertx.ext.shell.ShellServiceOptions
import io.vertx.ext.shell.command.CommandRegistry
import io.vertx.ext.shell.term.HttpTermOptions
import io.vertx.ext.shell.term.TelnetTermOptions
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.ext.shell.startAwait

class ShellVerticle : CoroutineVerticle() {

    override suspend fun start() {
        val shellServiceOptions = ShellServiceOptions()
            .setWelcomeMessage(" Welcome to the piveau-hub CLI!")
            .setSessionTimeout(21600000).apply {
                config.asJsonObject("PIVEAU_HUB_SHELL_CONFIG").forEach {
                    val options = it.value as JsonObject
                    when (it.key) {
                        "telnet" -> telnetOptions = TelnetTermOptions()
                            .setHost(options.getString("host", "0.0.0.0"))
                            .setPort(options.getInteger("port", 5000))
                        "http" -> httpOptions = HttpTermOptions()
                            .setHost(options.getString("host", "0.0.0.0"))
                            .setPort(options.getInteger("port", 8085))
                    }
                }
            }

        ShellService.create(vertx, shellServiceOptions).startAwait()

        CommandRegistry.getShared(vertx)
            .registerCommand(SyncIndexCommand.create(vertx))
            .registerCommand(ClearCatalogueCommand.create(vertx))
            .registerCommand(RepairCatalogueCommand.create(vertx))
            .registerCommand(LaunchCatalogueCommand.create(vertx))
    }

}
