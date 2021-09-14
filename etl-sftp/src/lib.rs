use ssh2::Sftp;
pub enum Credentials {
    UserPassword {
        username: String,
        pw: String,
    },
    SshKeyPath {
        username: String,
        ssh_key_path: String,
    },
}

/*
// may be needed for those which do not support keyboard entry
impl KeyboardInteractivePrompt for Credentials {
    fn prompt<'a>(
        &mut self,
        username: &str,
        instructions: &str,
        prompts: &[Prompt<'a>],
    ) -> Vec<String> {
        println!("username {}", username);
        Vec::new()
    }
}
*/

pub fn ssh_connect(url: &str, creds: Credentials) -> Result<Sftp, ssh2::Error> {
    use ssh2::Session;
    use std::net::TcpStream;
    use std::net::ToSocketAddrs;
    let mut addr = url.to_socket_addrs().expect("Given bad url");
    let addr1 = addr.next().expect("Could not generate propper address");
    let tcp = TcpStream::connect(addr1).expect("Error starting stream");
    let mut sess = Session::new()?;
    match creds {
        Credentials::UserPassword { username, pw } => {
            sess.set_tcp_stream(tcp);
            sess.handshake()?;
            sess.userauth_password(&username, &pw)?;
            sess.sftp()
        }
        Credentials::SshKeyPath {
            username,
            ssh_key_path,
        } => {
            let mut agent = sess.agent()?;
            agent.connect()?;
            agent.list_identities()?;
            let list = agent.identities()?;
            let key = list
                .iter()
                // find the key
                .find(|i| {
                    //println!("see key {}", i.comment());
                    i.comment().contains(&ssh_key_path)
                })
                .expect("The supplied key path did not match, try using ssh-add");
            sess.set_tcp_stream(tcp);
            sess.handshake()?;
            agent.userauth(&username, &key)?;
            sess.sftp()
        }
    }
}
